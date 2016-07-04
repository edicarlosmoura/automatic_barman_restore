import os
import sys
import ConfigParser
import subprocess
import time
import datetime
import psycopg2

def __run__(params):

	conffile = "restore.ini"
	if not os.path.isfile(conffile):
		print "Problem loading configuration file"
		sys.exit(1)

	config = ConfigParser.ConfigParser()
	config.readfp(open(conffile))
	database = config.get('restore','database')
	backup_host = config.get('restore','backup_host')
	pgsql_version = config.get('restore','pgsql_version')
	target = config.get('restore','target')
	restore_dir = config.get('restore','restore_dir')
	
	datadir_dest = config.get('master_config','datadir_dest')
	barman_host = config.get('master_config','barman_host')
	restore_host = config.get('master_config','restore_host')
	ip_barman = config.get('master_config','ip_barman')
	ip_pgrestore = config.get('master_config','ip_pgrestore')


	def validaRestore():

		if backup_host == restore_host:
			print "Same Backup host and Restore host"
			print "Backup host: {0}  -  Restore host: {1} ".format(backup_host, restore_host)
			rmlock()
		else:
			print "- Restore host OK"
			return "TRUE"

	def getBackup(target):

		# Get backup list
		command = 'barman list-backup ' + backup_host
		p = subprocess.Popen(['ssh', 'barman@'+ip_barman, command], shell=False, stdout=subprocess.PIPE)
		list_backup = p.communicate()[0]
		
		target = datetime.datetime.strptime(target, "%Y-%m-%d %H:%M:%S")
		for line in list_backup.splitlines():
			x = line.split(' - ')[1]
			y = datetime.datetime.strptime(x, '%c')
			if y <= target:
				return line.split(' - ')[0]
				break

	def executeRestore():

		if getBackup() != "FALSE":

			cleanXlog()

			actionPostgres('stop')
			print "- Restore in progress\n"
			print "- Restore process can be time consuming, please waiting conclusion\n"

			command = 'barman recover --remote-ssh-command="ssh postgres@' + ip_pgrestore + '" --target-time "' + target + '" ' + getBackup(target) + ' ' + datadir_dest
			p = subprocess.Popen(['ssh', 'barman@'+ip_barman, command], shell=False, stdout=subprocess.PIPE)
			result = p.communicate()[0]
			
			if actionPostgres('start') == "TRUE":
				while testUpPostgres() == "FALSE":
					time.sleep(10)
				executePgdump()
		else:
			print "- Backup requested this out of retention"
			rmlock()

	def actionPostgres(action):
		print '- Effecting '+action+' local PostgreSQL service.'
		p = subprocess.Popen(['sudo', 'systemctl', action, pgsql_version+'.service'],  stdout=subprocess.PIPE)
		result = p.communicate()[0]

		return "TRUE"

	def cleanXlog():
		print "- Removing Xlogs"
		barman_log = datadir_dest + 'barman_xlog'
		if os.path.exists(barman_log):
			p = subprocess.Popen(['rm', '-r', '-f', datadir_dest+'barman_xlog'],  stdout=subprocess.PIPE)
			result = p.communicate()[0]

	def executePgdump():
		backup_dir = '/home/restore/'+ restore_dir
		if not os.path.exists(backup_dir):
			print "- Creating pg_dump dir"
			p = subprocess.Popen(['mkdir', '-p', backup_dir],  stdout=subprocess.PIPE)
			result = p.communicate()[0]
		
		print "- Executing Pg_dump"
		p = subprocess.Popen(['pg_dump', database, '-f', backup_dir+'/'+database+'_'+date_backup+hour_backup+'.sql'],  stdout=subprocess.PIPE)
		result = p.communicate()[0]
		rmlock()

		print "- Backup Done!\n"
		print "- Database: "+database
		print "- Backup Host: "+backup_host
		print "- Date of Backup: "+date_backup+" - Hour of Backup: "+hour_backup
		print "- Path pg_dump file: "+backup_dir+'/'+database+'_'+date_backup+hour_backup+'.sql'

	def lock():
		filelock = '/tmp/restore.lock'
		if os.path.isfile(filelock):
			print "- Another restore process is running!"
		else:
			p = subprocess.Popen(['touch', filelock],  stdout=subprocess.PIPE)
			result = p.communicate()[0]
			return "TRUE"

	def rmlock():
		p = subprocess.Popen(['rm', '-f', '/tmp/restore.lock'],  stdout=subprocess.PIPE)
		result = p.communicate()[0]

        def testUpPostgres():
                try:
                        conn = psycopg2.connect('user=postgres')
                except Exception, e:
                        return "FALSE"

if lock() == "TRUE":
	if validaRestore() == "TRUE":
		if getBackup(target) != "FALSE":
			executeRestore()
	

if __name__ == '__main__':
    print __run__({})
