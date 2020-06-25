set -x
sudo ntpdate us.pool.ntp.org
sudo tzupdate
set +x
HERE=${PWD}
cd /traveler-integrated
git pull
cd /JetLag
git pull
cd $HERE
if [ ! -r Demo.ipynb ]
then
    cp /JetLag/notebooks/Demo.ipynb .
fi
(cd /traveler-integrated && python3 serve.py > ./serve.log 2>&1) &
jupyter notebook --ip 0.0.0.0 --port 8789 --no-browser
