FROM stevenrbrandt/phylanx.devenv:working

USER root

# Set up jupyter
RUN pip3 install jupyter requests

WORKDIR /
RUN git clone https://github.com/hdc-arizona/traveler-integrated
WORKDIR /traveler-integrated
RUN find . | xargs chown jovyan
RUN pip3 install -r requirements.txt

WORKDIR /
RUN git clone https://github.com/STEllAR-GROUP/JetLag.git
WORKDIR /JetLag
RUN find . | xargs chown jovyan

RUN chown jovyan ~jovyan/.bashrc
RUN dnf install -y jq
WORKDIR /
ENV LC_ALL en_US.utf8

USER jovyan

# Fix otf2 path
ENV PATH="/usr/local/otf2/bin:${PATH}"
WORKDIR /home/jovyan
RUN git clone https://github.com/stevenrbrandt/workenv.git
WORKDIR /home/jovyan/workenv
RUN python3 install.py
ENV PATH /usr/local/cli/bin:/traveler-integrated/notebook/agave-cli/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
ENV PYTHONPATH /JetLag


# Default container command is to launch jupyter
RUN mkdir -p /home/jovyan/workdir
WORKDIR /home/jovyan/workdir
CMD jupyter notebook --ip 0.0.0.0 --port 8789 --no-browser
