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
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

import io.mycat.util.SelectorUtil;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.MycatServer;
import io.mycat.net.factory.FrontendConnectionFactory;

/**
 * 处理的是 Accept 事件，是服务端接收客户端连接事件，就是 MyCAT 作为服务端去处理前端
 * 业务程序发过来的连接请求。
 * @author mycat
 */
public final class NIOAcceptor extends Thread implements SocketAcceptor{
	private static final Logger LOGGER = LoggerFactory.getLogger(NIOAcceptor.class);
	private static final AcceptIdGenerator ID_GENERATOR = new AcceptIdGenerator();

	private final int port;
	// 事件选择器
	private volatile Selector selector;
	// 监听新进来的 TCP 连接的通道
	private final ServerSocketChannel serverChannel;
	private final FrontendConnectionFactory factory;
	private long acceptCount;
	// 当连接建立后，从 reactorPool 中分配一个 NIOReactor，处理 Read 和 Write 事件
	private final NIOReactorPool reactorPool;

	/**
	 * 监听通道在 NIOAcceptor 构造函数里启动,然后注册到实际进行任务处理的 Dispather 线程的 Selector 中
	 *
	 * @param name
	 * @param bindIp
	 * @param port
	 * @param factory
	 * @param reactorPool
	 * @throws IOException
	 */
	public NIOAcceptor(String name, String bindIp,int port,
			FrontendConnectionFactory factory, NIOReactorPool reactorPool)
			throws IOException {
		super.setName(name);
		this.port = port;
		this.selector = Selector.open();
		this.serverChannel = ServerSocketChannel.open();
		this.serverChannel.configureBlocking(false);
		/** 设置TCP属性 */
		serverChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
		serverChannel.setOption(StandardSocketOptions.SO_RCVBUF, 1024 * 16 * 2);
		// backlog=100
		serverChannel.bind(new InetSocketAddress(bindIp, port), 100);
		this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
		this.factory = factory;
		this.reactorPool = reactorPool;
	}

	public int getPort() {
		return port;
	}

	public long getAcceptCount() {
		return acceptCount;
	}

	/**
	 * selector 不断监听连接事件，然后在 accept()函数中对事件进行处理。
	 *
	 */
	@Override
	public void run() {
		int invalidSelectCount = 0;
		for (;;) {
			final Selector tSelector = this.selector;
			++acceptCount;
			try {
				long start = System.nanoTime();
			    tSelector.select(1000L);
				long end = System.nanoTime();
				Set<SelectionKey> keys = tSelector.selectedKeys();
				if (keys.size() == 0 && (end - start) < SelectorUtil.MIN_SELECT_TIME_IN_NANO_SECONDS )
				{
					invalidSelectCount++;
				}
				else
                {
					try {
						for (SelectionKey key : keys) {
							if (key.isValid() && key.isAcceptable()) {
								// connect
								accept();
							} else {
								key.cancel();
							}
						}
					} finally {
						keys.clear();
						invalidSelectCount = 0;
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
				LOGGER.warn(getName(), e);
			}
		}
	}

	/**
	 * 当连接建立完毕后，从
	 * reactorPool 中获得一个 NIOReactor，然后把连接传递到 NIOReactor，然后后续的 Read 和 Write 事件就交给
	 * NIOReactor 处理了。
	 */
	private void accept() {
		SocketChannel channel = null;
		try {
			channel = serverChannel.accept();
			channel.configureBlocking(false);
			FrontendConnection c = factory.make(channel);
			c.setAccepted(true);
			c.setId(ID_GENERATOR.getId());
			// 轮询获取processor(BusinessExecutor)
			NIOProcessor processor = (NIOProcessor) MycatServer.getInstance()
					.nextProcessor();
			c.setProcessor(processor);

			// 轮询获取nioreactor
			NIOReactor reactor = reactorPool.getNextReactor();
			// 插入到nioreactor中的连接队列, 并唤醒该线程(阻塞在select上)
			reactor.postRegister(c);

		} catch (Exception e) {
	        LOGGER.warn(getName(), e);
			closeChannel(channel);
		}
	}

	private static void closeChannel(SocketChannel channel) {
		if (channel == null) {
			return;
		}
		Socket socket = channel.socket();
		if (socket != null) {
			try {
				socket.close();
			} catch (IOException e) {
		       LOGGER.error("closeChannelError", e);
			}
		}
		try {
			channel.close();
		} catch (IOException e) {
            LOGGER.error("closeChannelError", e);
		}
	}

	/**
	 * 前端连接ID生成器
	 * 
	 * @author mycat
	 */
	private static class AcceptIdGenerator {

		private static final long MAX_VALUE = 0xffffffffL;

		private long acceptId = 0L;
		private final Object lock = new Object();

		private long getId() {
			synchronized (lock) {
				if (acceptId >= MAX_VALUE) {
					acceptId = 0L;
				}
				return ++acceptId;
			}
		}
	}



}
