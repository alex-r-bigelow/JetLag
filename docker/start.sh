(cd /traveler-integrated && python3 serve.py > ./serve.log 2>&1)
jupyter notebook --ip 0.0.0.0 --port 8789 --no-browser
