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
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import java.util.concurrent.atomic.AtomicLong;

import io.mycat.util.SelectorUtil;

/**
 * MyCAT 作为客户端去主动连接 MySQL Server
 *
 * @author mycat
 */
public final class NIOConnector extends Thread implements SocketConnector {
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOConnector.class);
	public static final ConnectIdGenerator ID_GENERATOR = new ConnectIdGenerator();

	private final String name;
	// 事件选择器
	private volatile Selector selector;
	// 需要建立连接的对象，临时放在这个队列里
	private final BlockingQueue<AbstractConnection> connectQueue;
	private long connectCount;
	// 当连接建立后，从 reactorPool 中分配一个 NIOReactor，处理 Read 和 Write 事件
	private final NIOReactorPool reactorPool;

	public NIOConnector(String name, NIOReactorPool reactorPool)
			throws IOException {
		super.setName(name);
		this.name = name;
		this.selector = Selector.open();
		this.reactorPool = reactorPool;
		this.connectQueue = new LinkedBlockingQueue<AbstractConnection>();
	}

	public long getConnectCount() {
		return connectCount;
	}

    /**
     * 作用: 把需要建立的连接放到 connectQueue 队列中，然后再唤醒 selector。
     * 触发: 在新建连接或者心跳时被 XXXXConnectionFactory 触发的
     * @param c
     */
	public void postConnect(AbstractConnection c) {
		connectQueue.offer(c);
		selector.wakeup();
	}

	@Override
	public void run() {
		int invalidSelectCount = 0;
		for (;;) {
			final Selector tSelector = this.selector;
			++connectCount;
			try {
				long start = System.nanoTime();
				tSelector.select(1000L);
				long end = System.nanoTime();
				connect(tSelector);
				Set<SelectionKey> keys = tSelector.selectedKeys();
				if (keys.size() == 0 && (end - start) < SelectorUtil.MIN_SELECT_TIME_IN_NANO_SECONDS )
				{
					invalidSelectCount++;
				}
				else
				{
					try {
						for (SelectionKey key : keys)
						{
							Object att = key.attachment();
							if (att != null && key.isValid() && key.isConnectable())
							{
								finishConnect(key, att);
							} else
							{
								key.cancel();
							}
						}
					} finally
					{
						invalidSelectCount = 0;
						keys.clear();
					}
				}
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
			}
		}
	}

    /**
     * connect 函数的目的就是处理 postConnect 函数操作的 connectQueue 队列：
     * 1. 判断 connectQueue 中是否新的连接请求
     * 2. 建立一个 SocketChannel
     * 3. 在 selector 中进行注册 OP_CONNECT
     * 4. 发起 SocketChannel.connect()操作
     *
     * @param selector
     */
	private void connect(Selector selector) {
		AbstractConnection c = null;
		while ((c = connectQueue.poll()) != null) {
			try {
				SocketChannel channel = (SocketChannel) c.getChannel();
				channel.register(selector, SelectionKey.OP_CONNECT, c);
				channel.connect(new InetSocketAddress(c.host, c.port));

			} catch (Exception e) {
				LOGGER.error("error:",e);
				c.close(e.toString());
			}
		}
	}

    /**
     * 当连接建立完毕后，从 reactorPool 中获得一个 NIOReactor，然后把连
     * 接传递到 NIOReactor，然后后续的 Read 和 Write 事件就交给 NIOReactor 处理了。
     *
     * @param key
     * @param att
     */
	private void finishConnect(SelectionKey key, Object att) {
		BackendAIOConnection c = (BackendAIOConnection) att;
		try {
			if (finishConnect(c, (SocketChannel) c.channel)) {
				clearSelectionKey(key);
				c.setId(ID_GENERATOR.getId());
				NIOProcessor processor = MycatServer.getInstance()
						.nextProcessor();
				c.setProcessor(processor);
				NIOReactor reactor = reactorPool.getNextReactor();
				reactor.postRegister(c);
				c.onConnectfinish();
			}
		} catch (Exception e) {
			clearSelectionKey(key);
			LOGGER.error("error:",e);
			c.close(e.toString());
			c.onConnectFailed(e);

		}
	}

	private boolean finishConnect(AbstractConnection c, SocketChannel channel)
			throws IOException {
		if (channel.isConnectionPending()) {
			channel.finishConnect();

			c.setLocalPort(channel.socket().getLocalPort());
			return true;
		} else {
			return false;
		}
	}

	private void clearSelectionKey(SelectionKey key) {
		if (key.isValid()) {
			key.attach(null);
			key.cancel();
		}
	}

	/**
	 * 后端连接ID生成器
	 *
	 * @author mycat
	 */
	public static class ConnectIdGenerator {

		private static final long MAX_VALUE = Long.MAX_VALUE;
		private AtomicLong connectId = new AtomicLong(0);

		public long getId() {
			return connectId.incrementAndGet();
		}
	}

}
