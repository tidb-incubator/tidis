FROM centos:7.6.1810 as builder

RUN yum install -y epel-release && \
    yum clean all && \
    yum makecache

RUN yum install -y centos-release-scl && \
    yum install -y \
      devtoolset-8 \
      perl cmake3 && \
    yum clean all

# CentOS gives cmake 3 a weird binary name, so we link it to something more normal
# This is required by many build scripts, including ours.
RUN ln -s /usr/bin/cmake3 /usr/bin/cmake
ENV LIBRARY_PATH /usr/local/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH /usr/local/lib:$LD_LIBRARY_PATH

# Install Rustup
RUN curl https://sh.rustup.rs -sSf | sh -s -- --no-modify-path --default-toolchain none -y
ENV PATH /root/.cargo/bin/:$PATH

# Install the Rust toolchain
WORKDIR /tikv
COPY rust-toolchain ./
RUN rustup self update \
  && rustup set profile minimal \
  && rustup default $(cat "rust-toolchain")

COPY src ./src/
COPY Cargo* ./
COPY rust-toolchain ./
COPY Makefile ./
RUN source /opt/rh/devtoolset-8/enable && make release

FROM centos:7.6.1810

COPY --from=builder /tikv/target/release/tikv-service-server /tikv-service-server

EXPOSE 6666 6443 8080

ENTRYPOINT ["/tikv-service-server"]
