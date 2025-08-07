#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM hub.hmf.xyz/base/dolphinscheduler-baseos:8 AS worker-base

ENV DOLPHINSCHEDULER_HOME /opt/dolphinscheduler

WORKDIR $DOLPHINSCHEDULER_HOME

ADD ./target/worker-server $DOLPHINSCHEDULER_HOME


FROM worker-base AS worker-chunjun

ADD ./src/main/docker/downloads/chunjun-dist-*.tar.gz /opt


FROM worker-chunjun AS worker-flink

ADD ./src/main/docker/downloads/flink-1.13.6-bin-scala_2.12.tgz /opt


FROM worker-flink AS worker-hadoop

ADD ./src/main/docker/downloads/hadoop-*.tar.gz /opt

FROM hub.hmf.xyz/base/dolphinscheduler-baseos:8 AS worker-server

COPY --from=worker-hadoop /opt/dolphinscheduler /opt/dolphinscheduler
COPY --from=worker-hadoop /opt/chunjun /opt/chunjun
COPY --from=worker-hadoop /opt/flink-1.13.6/bin /opt/flink/bin
COPY --from=worker-hadoop /opt/flink-1.13.6/lib /opt/flink/lib
COPY --from=worker-hadoop /opt/flink-1.13.6/conf /opt/flink/conf
COPY --from=worker-hadoop /opt/hadoop-3.3.6/share/hadoop/common /opt/hadoop/share/hadoop/common
COPY --from=worker-hadoop /opt/hadoop-3.3.6/share/hadoop/hdfs /opt/hadoop/share/hadoop/hdfs
COPY --from=worker-hadoop /opt/hadoop-3.3.6/share/hadoop/yarn /opt/hadoop/share/hadoop/yarn

ENV DOCKER true
ENV TZ Asia/Shanghai
ENV DOLPHINSCHEDULER_HOME /opt/dolphinscheduler

WORKDIR $DOLPHINSCHEDULER_HOME

EXPOSE 1235

CMD [ "/bin/bash", "./bin/start.sh" ]