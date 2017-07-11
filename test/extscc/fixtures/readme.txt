Add this line to /etc/hosts:
127.0.0.1	peer0.org1.example.com peer0.org2.example.com

Start Fabric
(source .env && docker-compose down && docker-compose up --force-recreate)
