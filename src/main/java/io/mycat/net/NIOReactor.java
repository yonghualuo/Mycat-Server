/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.net;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.mycat.util.SelectorUtil;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

/**
 * 网络事件反应器
 * 
 * <p>
 * Catch exceptions such as OOM so that the reactor can keep running for response client!
 * </p>
 * @since 2016-03-30
 * 
 * @author mycat, Uncle-pan
 * 
 */
public final class NIOReactor {
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOReactor.class);
	private final String name;
	private final RW reactorR;

	public NIOReactor(String name) throws IOException {
		this.name = name;
		this.reactorR = new RW();
	}

	final void startup() {
		new Thread(reactorR, name + "-RW").start();
	}

    /**
     * 把 AbstractConnection 对象加入缓冲队列,然后 wakeup selector 等待注册
     *
     * 直接注册不可吗? 不是不可以,是效率问题，至少加两次锁,锁竞争激烈
     * - Channel 本身的 regLock,竞争几乎没有
     * - Selector 内部的 key 集合,竞争激烈
     * 更好的方式就是采用上面这种方式，先放入缓冲队列，等待 selector 单线程进行注册。
     * @param c
     */
	final void postRegister(AbstractConnection c) {
		reactorR.registerQueue.offer(c);
		reactorR.selector.wakeup();
	}

	final Queue<AbstractConnection> getRegisterQueue() {
		return reactorR.registerQueue;
	}

	final long getReactCount() {
		return reactorR.reactCount;
	}

	private final class RW implements Runnable {
	    // 是一个 dispatcher，用来负责多个链路事件的事件分发
		private volatile Selector selector;
		private final ConcurrentLinkedQueue<AbstractConnection> registerQueue;
		private long reactCount;

		private RW() throws IOException {
			this.selector = Selector.open();
			this.registerQueue = new ConcurrentLinkedQueue<AbstractConnection>();
		}


        /**
         * 1. 注册 OP_READ 事件
         * OP_WRITE 事件的注册放在 NIOSocketWR.doNextWriteCheck()函数中，doNextWriteCheck 既被
         * selector 线程调用，也会被其它的业务线程调用，此时就会存在 lock 竞争的问题，所以对于 OP_WRITE 事件也
         * 建议用队列缓存的方式，不过对于 MyCAT 的流量场景，大部分写操作是由业务线程直接写入，只有在网络繁忙
         * 时，业务线程不能一次全部写完，才会通过 OP_WRITE 注册方式进行候补写。
         * 2. selector 监听事件，如果是读事件，就调用 con.asynRead()函数，进行字节的读取。对于 asynRead 中如
         * 何提取 MySQL 协议包，就属于网络框架讨论的内容，
         * 3. selector 监听到写事件，调用 AbstractConnection.doNextWriteCheck()进行写事件的处理，在
         * AbstractConnection.doNextWriteCheck()中，又调用 NIOSocketWR.doNextWriteCheck()进行处理的
         */
		@Override
		public void run() {
			int invalidSelectCount = 0;
			Set<SelectionKey> keys = null;
			for (;;) {
				++reactCount;
				try {
					final Selector tSelector = this.selector;
					long start = System.nanoTime();
					tSelector.select(500L);
					long end = System.nanoTime();
					// RW的registerQueue中的conn注册到tSelector
					register(tSelector);
					keys = tSelector.selectedKeys();
					if (keys.size() == 0 && (end - start) < SelectorUtil.MIN_SELECT_TIME_IN_NANO_SECONDS )
					{
						invalidSelectCount++;
					}
					else
					{
						invalidSelectCount = 0;
						for (SelectionKey key : keys) {
							AbstractConnection con = null;
							try {
								Object att = key.attachment();
								if (att != null) {
									con = (AbstractConnection) att;
									if (key.isValid() && key.isReadable()) {
										try {
											con.asynRead();
										} catch (IOException e) {
											con.close("program err:" + e.toString());
											continue;
										} catch (Exception e) {
											LOGGER.warn("caught err:", e);
											con.close("program err:" + e.toString());
											continue;
										}
									}
									if (key.isValid() && key.isWritable()) {
										con.doNextWriteCheck();
									}
								} else {
									key.cancel();
								}
							} catch (CancelledKeyException e) {
								if (LOGGER.isDebugEnabled()) {
									LOGGER.debug(con + " socket key canceled");
								}
							} catch (Exception e) {
								LOGGER.warn(con + " " + e);
							} catch (final Throwable e) {
								// Catch exceptions such as OOM and close connection if exists
								//so that the reactor can keep running!
								// @author Uncle-pan
								// @since 2016-03-30
								if (con != null) {
									con.close("Bad: " + e);
								}
								LOGGER.error("caught err: ", e);
								continue;
							}
						}
					}
					// select达到阈值, 重建selector
					if (invalidSelectCount > SelectorUtil.REBUILD_COUNT_THRESHOLD)
					{
						final Selector rebuildSelector = SelectorUtil.rebuildSelector(this.selector);
						if (rebuildSelector != null)
						{
							this.selector = rebuildSelector;
						}
						invalidSelectCount = 0;
					}
				} catch (Exception e) {
					LOGGER.warn(name, e);
				} catch (final Throwable e){
					// Catch exceptions such as OOM so that the reactor can keep running!
                	// @author Uncle-pan
                	// @since 2016-03-30
					LOGGER.error("caught err: ", e);
				} finally {
					if (keys != null) {
						keys.clear();
					}

				}
			}
		}

		private void register(Selector selector) {
			AbstractConnection c = null;
			if (registerQueue.isEmpty()) {
				return;
			}
			while ((c = registerQueue.poll()) != null) {
				try {
					// 注册OP_READ事件
					((NIOSocketWR) c.getSocketWR()).register(selector);
					c.register();
				} catch (Exception e) {
					c.close("register err" + e.toString());
				}
			}
		}

	}

}
