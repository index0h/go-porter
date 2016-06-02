FROM scratch

ADD bin/porter /porter

CMD ["/porter"]