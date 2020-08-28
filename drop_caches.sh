{
	sudo hdparm -f /dev/sda /dev/nvme1n1
	echo 3 | sudo tee /proc/sys/vm/drop_caches
} > /dev/null
