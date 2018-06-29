package io.mycat.net;

import java.io.IOException;

/**
 * 读写操作类
 */
public abstract class SocketWR {
	public abstract void asynRead() throws IOException;
	public abstract void doNextWriteCheck() ;
}
