package io.mycat.net;

/**
 * 接收连接请求类
 */
public interface SocketAcceptor {

	void start();

	String getName();

	int getPort();

}
