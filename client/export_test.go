package client

// SetCtlBufSize makes ctl replies read size bytes at a time until restore.
func SetCtlBufSize(size int) (restore func()) {
	old := ctlBufs
	ctlBufs = newCtlBufs(size)
	return func() { ctlBufs = old }
}
