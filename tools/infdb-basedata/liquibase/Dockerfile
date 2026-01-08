FROM liquibase/liquibase:latest
RUN lpm add postgresql --global

WORKDIR /liquibase
COPY changelog/ /liquibase/changelog
COPY changelog/changelog-master.yml /liquibase/changelog-master.yml
CMD ["liquibase", "update"]