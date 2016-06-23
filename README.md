# automatic_barman_restore
Script python to automatic restore database on another host when you used PostgreSQL Barman Backup

It's necessary: 
- The PostgreSQL Barman Backup it's running
- The restore host network comunicate with barman host
- Shared ssh key barman and postgres user
- systemd
