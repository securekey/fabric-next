Add this line to /etc/hosts:
127.0.0.1	peer0.org1.example.com peer0.org2.example.com

Start Fabric
Run 'make clean' in fabric-sdk-go
(source .env && docker-compose up --force-recreate)
