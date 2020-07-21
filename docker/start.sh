set -x
sudo ntpdate us.pool.ntp.org
sudo /usr/local/bin/tzupdate
HERE=${PWD}
cd /traveler-integrated
git pull
cd /JetLag
git pull
cd $HERE
cp -n /JetLag/notebooks/*.ipynb .
(cd /traveler-integrated && python3 serve.py > ./serve.log 2>&1) &
jupyter notebook --ip 0.0.0.0 --port 8789 --no-browser
