FROM stevenrbrandt/phylanx.rel.test:working
USER root
RUN dnf install -y curl findutils vim git which jq gcc gcc-c++ sudo glibc-langpack-en otf2 ntpdate procps-ng

# Set up jupyter
RUN pip3 install jupyter requests numpy scipy matplotlib tzupdate email-validator gui_fun

WORKDIR /
RUN git clone -b ui-overhaul https://github.com/hdc-arizona/traveler-integrated
WORKDIR /traveler-integrated
RUN find . | xargs chown jovyan
RUN pip3 install -r requirements.txt
WORKDIR /traveler-integrated/profiling_tools/clibs
RUN python3 rp_extension_build.py
RUN mv _cCalcBin.*.so ..

WORKDIR /
RUN git clone --branch ui-overhaul https://github.com/alex-r-bigelow/JetLag.git
WORKDIR /JetLag
RUN find . | xargs chown jovyan

WORKDIR /traveler-integrated
#RUN dnf install -y patch
#RUN patch -p1 < /JetLag/docker/patch.txt

RUN chown jovyan ~jovyan/.bashrc
WORKDIR /
ENV LC_ALL en_US.utf8

USER jovyan
RUN mkdir -p /home/jovyan/workdir

# Fix otf2 path
ENV PATH="/usr/local/otf2/bin:${PATH}"
WORKDIR /home/jovyan
RUN git clone https://github.com/stevenrbrandt/workenv.git
WORKDIR /home/jovyan/workenv
RUN python3 install.py
ENV PATH /usr/local/cli/bin:/traveler-integrated/notebook/agave-cli/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
ENV PYTHONPATH /JetLag
ENV PYTHONUSERBASE /usr/local/userbase

WORKDIR /home/jovyan/workdir

# Default container command is to launch jupyter
COPY start.sh /usr/local/bin/start.sh
CMD bash /usr/local/bin/start.sh
