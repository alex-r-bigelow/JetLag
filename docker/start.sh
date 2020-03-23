echo -n 'ntpdate: '
sudo ntpdate us.pool.ntp.org
echo -n 'tzupdate: '
sudo tzupdate
HERE=${PWD}
cd /traveler-integrated
git pull
cd /JetLag
git pull
cd $HERE
(cd /traveler-integrated && python3 serve.py > ./serve.log 2>&1) &
jupyter notebook --ip 0.0.0.0 --port 8789 --no-browser
