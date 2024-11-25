# ChatDB
TODO:

Mysql part:

bugs in patteren matching, more patterns to be added

Sample data generation

Mongodb part:


Supporting Commands:

list databases

use database name

upload xxx.csv

list tables

introduce tablename

queries: please refer to template part

## Mongodb

keywords:

find:

db.products.find({ price: { $gt: 100 } }) 

Projection:

db.products.find({ category: "electronics" }, { name: 1, price: 1, _id: 0 })

Aggregation (Grouping):
db.sales.aggregate([
  { $group: { _id: "$store", totalSales: { $sum: "$amount" } } }
])

lookup:

db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      localField: "productId",
      foreignField: "_id",
      as: "productDetails"
    }
  }
])

Unwind:

db.orders.aggregate([
  { $unwind: "$items" }
])

Sort:

db.products.find().sort({ price: -1 })

count:

db.person.count({age: {$gt: 25}})
## [chatgpt hints](https://chatgpt.com/share/673adcca-8038-800a-97b3-bab2c15c48d1)
