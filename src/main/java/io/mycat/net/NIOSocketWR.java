package io.mycat.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import io.mycat.util.TimeUtil;

public class NIOSocketWR extends SocketWR {
	private SelectionKey processKey;
	private static final int OP_NOT_READ = ~SelectionKey.OP_READ;
	private static final int OP_NOT_WRITE = ~SelectionKey.OP_WRITE;
	private final AbstractConnection con;
	private final SocketChannel channel;
	private final AtomicBoolean writing = new AtomicBoolean(false);

	public NIOSocketWR(AbstractConnection con) {
		this.con = con;
		this.channel = (SocketChannel) con.channel;
	}

	public void register(Selector selector) throws IOException {
		try {
			processKey = channel.register(selector, SelectionKey.OP_READ, con);
		} finally {
			if (con.isClosed.get()) {
				clearSelectionKey();
			}
		}
	}

    /**
     * 1.若通道空闲当前线程直接写，否则缓存
     * 队列，注册 OP_Write 事件；
     * 2.通过 seletor 线程循环检查写事件是否就
     * 绪
     *
     * - - - - - - - - - - - - - - - - -
     *
     * 1. 先判断是否正在写，如果正在写，退出（之前已经把写内容放到缓冲队列，那么此处是否可以优化呢，即
     * 当发送缓冲队列为空的时候,可以直接往 channel 写数据，不能写再放缓冲队列，理论上可以优化，但是写代码时
     * 要注意，因为必需要保证协议包的顺序，还要考虑到前一次写时，是否有 buffer 没有写完，若前一次写入时，最
     * 后一个 buffer 没有写完，记得退回缓冲队列；MyCAT 当前的实现方式是增加了一个变量专门存放上次未写完的
     * buffer）
     * 2. write0()方法是只要 buffer 中还有，就不停写入；直到写完所有 buffer，或者写入时，返回写入字节为
     * 零，表示网络繁忙，就回临时退出写操作。
     * 3. 没有完全写入并且缓冲队列为空,取消注册写事件
     * 4. 没有完全写入或者缓冲队列有代写对象,继续注册写时间
     * 5. 特别说明，writing.set(false)必须要在 boolean noMoreData = write0()之后和 if (noMoreData &&
     * con.writeQueue.isEmpty())之前，否则会导致当网络流量较低时，消息包缓存在内存中迟迟发不出去的现象。
     */
	@Override
    public void doNextWriteCheck() {

		if (!writing.compareAndSet(false, true)) {
			return;
		}

		try {
			boolean noMoreData = write0();
			writing.set(false);
			if (noMoreData && con.writeQueue.isEmpty()) {
				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) != 0)) {
					disableWrite();
				}

			} else {

				if ((processKey.isValid() && (processKey.interestOps() & SelectionKey.OP_WRITE) == 0)) {
					enableWrite(false);
				}
			}

		} catch (IOException e) {
			if (AbstractConnection.LOGGER.isDebugEnabled()) {
				AbstractConnection.LOGGER.debug("caught err:", e);
			}
			con.close("err:" + e);
		}

	}

	private boolean write0() throws IOException {

		int written = 0;
		ByteBuffer buffer = con.writeBuffer;
		if (buffer != null) {
		    // position < limit
			while (buffer.hasRemaining()) {
				written = channel.write(buffer);
				// record writing status
				if (written > 0) {
					con.netOutBytes += written;
					con.processor.addNetOutBytes(written);
					con.lastWriteTime = TimeUtil.currentTimeMillis();
				} else {
					break;
				}
			}

			if (buffer.hasRemaining()) {
				con.writeAttempts++;
				return false;
			} else {
				con.writeBuffer = null;
				// recycle buffer
				con.recycle(buffer);
			}
		}
		while ((buffer = con.writeQueue.poll()) != null) {
			if (buffer.limit() == 0) {
				con.recycle(buffer);
				con.close("quit send");
				return true;
			}

			// 写模式切换到读模式, limit -> position, posion
			buffer.flip();
			try {
				while (buffer.hasRemaining()) {
					written = channel.write(buffer);// java.io.IOException:
									// Connection reset by peer
					if (written > 0) {
						con.lastWriteTime = TimeUtil.currentTimeMillis();
						con.netOutBytes += written;
						con.processor.addNetOutBytes(written);
						con.lastWriteTime = TimeUtil.currentTimeMillis();
					} else {
						break;
					}
				}
			} catch (IOException e) {
				con.recycle(buffer);
				throw e;
			}
			if (buffer.hasRemaining()) {
				con.writeBuffer = buffer;
				con.writeAttempts++;
				return false;
			} else {
				con.recycle(buffer);
			}
		}
		return true;
	}

    /**
     * 删除OP_WRITE
     */
	private void disableWrite() {
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() & OP_NOT_WRITE);
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("can't disable write " + e + " con "
					+ con);
		}

	}

    /**
     * 设置OP_WRITE
     * @param wakeup
     */
	private void enableWrite(boolean wakeup) {
		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
			needWakeup = true;
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("can't enable write " + e);

		}
		if (needWakeup && wakeup) {
			processKey.selector().wakeup();
		}
	}

	public void disableRead() {

		SelectionKey key = this.processKey;
		key.interestOps(key.interestOps() & OP_NOT_READ);
	}

	public void enableRead() {

		boolean needWakeup = false;
		try {
			SelectionKey key = this.processKey;
			key.interestOps(key.interestOps() | SelectionKey.OP_READ);
			needWakeup = true;
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("enable read fail " + e);
		}
		if (needWakeup) {
			processKey.selector().wakeup();
		}
	}

	private void clearSelectionKey() {
		try {
			SelectionKey key = this.processKey;
			if (key != null && key.isValid()) {
				key.attach(null);
				key.cancel();
			}
		} catch (Exception e) {
			AbstractConnection.LOGGER.warn("clear selector keys err:" + e);
		}
	}

	@Override
	public void asynRead() throws IOException {
		ByteBuffer theBuffer = con.readBuffer;
		// 分配bytebuffer
		if (theBuffer == null) {

			theBuffer = con.processor.getBufferPool().allocate(con.processor.getBufferPool().getChunkSize());

			con.readBuffer = theBuffer;
		}

		int got = channel.read(theBuffer);

		con.onReadData(got);
	}

}
