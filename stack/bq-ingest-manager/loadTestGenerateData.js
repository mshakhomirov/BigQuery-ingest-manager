const fs = require('fs');
const data = require("fs").readFileSync("./tmp/simple_transaction.csv", "utf8")
for (let i = 5; i < 300; i++) {
    fs.writeFileSync(`./data/simple_transaction${i}.csv`, data);
  } 
