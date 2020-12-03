FROM soajsorg/node

RUN mkdir -p /opt/soajs/mongo.sync/node_modules/
WORKDIR /opt/soajs/mongo.sync/
COPY . .
RUN npm install

CMD ["/bin/bash"]