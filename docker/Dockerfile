FROM fedora
RUN dnf install -y curl findutils vim git which jq python3 python3-pip gcc gcc-c++ python3-devel sudo glibc-langpack-en otf2 ntpdate procps-ng
RUN useradd -m jovyan
RUN echo "ALL            ALL = (ALL) NOPASSWD: ALL" >> /etc/sudoers

# Set up jupyter
RUN pip3 install jupyter requests numpy scipy matplotlib tzupdate email-validator

WORKDIR /
RUN git clone https://github.com/hdc-arizona/traveler-integrated
WORKDIR /traveler-integrated
RUN find . | xargs chown jovyan
RUN pip3 install -r requirements.txt
WORKDIR /traveler-integrated/profiling_tools/clibs
RUN python3 rp_extension_build.py
RUN mv _cCalcBin.*.so ..

WORKDIR /
RUN git clone https://github.com/STEllAR-GROUP/JetLag.git
WORKDIR /JetLag
RUN find . | xargs chown jovyan

RUN chown jovyan ~jovyan/.bashrc
WORKDIR /
ENV LC_ALL en_US.utf8

# Patch traveler
#WORKDIR /traveler-integrated
#RUN dnf install -y patch
#COPY patch2.txt ./
#RUN patch -p1 < patch2.txt

USER jovyan

# Fix otf2 path
ENV PATH="/usr/local/otf2/bin:${PATH}"
WORKDIR /home/jovyan
RUN git clone https://github.com/stevenrbrandt/workenv.git
WORKDIR /home/jovyan/workenv
RUN python3 install.py
ENV PATH /usr/local/cli/bin:/traveler-integrated/notebook/agave-cli/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
ENV PYTHONPATH /JetLag

RUN mkdir -p /home/jovyan/workdir
WORKDIR /home/jovyan/workdir

# Default container command is to launch jupyter
CMD bash /JetLag/docker/start.sh
