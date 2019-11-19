FROM spark-base:latest

ENV SPARK_MASTER_URL "spark://spark-master:7077"
ENV SPARK_SUBMIT_ARGS ""
ENV SPARK_APPLICATION_ARGS ""
ENV SPARK_APPLICATION_JAR_LOCATION file:///application/stop-bot-1.0.0-SNAPSHOT-all.jar
ENV SPARK_APPLICATION_MAIN_CLASS com.gd.StopBotApp

COPY . /application/

RUN chmod +x /application/stop-bot-1.0.0-SNAPSHOT-all.jar && \
    chmod +x /application/spark-submit.sh

CMD ["/bin/bash", "/application/spark-submit.sh"]
