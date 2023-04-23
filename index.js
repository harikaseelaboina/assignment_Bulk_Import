const express=require("express");
const app=express();
const { v4: uuidv4 } = require('uuid');
const {Pool,Client}=require("pg");
const {parse} = require('csv-parse');
const fs = require('fs');



const pool= new Pool({
    user:"postgres",
    host:"localhost",
    port:5432,
    password:"password",
    database:"BulkUsersDB"

})

// users table creation
pool.query("Create table users (id UUID PRIMARY KEY,name varchar(100),phone varchar(50),email varchar(100))",
(err,res)=>{
    if (err) {
        console.error('Error creating table', err.stack);
        
      } else {
        console.log('Table created successfully');
        
      }
})

// other information-adress table
pool.query("Create table address (name varchar(50),ipadress varchar(150),postalcode varchar(50))",
(err,res)=>{
    if (err) {
        console.error('Error creating table', err.stack);
        
      } else {
        console.log('Adress Table created successfully');
        
      }
})

// parsing data in user table from csv file
fs.createReadStream('MOCK_DATA.csv')
  .pipe(parse({ columns: true }))
  .on('data', (data) => {
    if (data.email) { // check if email exists
      const id = uuidv4();
      const query = {
        text: 'INSERT INTO users (id, name, phone, email) VALUES ($1, $2, $3, $4)',
        values: [id, data.name, data.phone, data.email]
      };

      pool.query(query)
        .then(() => {
          console.log(`Data for ${data.name} inserted successfully`);
        })
        .catch((error) => {
          console.error(`Error inserting data for ${data.name}`, error);
        });
    }
  })
  .on('end', () => {
    console.log('CSV file successfully processed');
  });


//parsing remaining values in adress table
  fs.createReadStream('MOCK_DATA.csv')
  .pipe(parse({ columns: true }))
  .on('data', (data) => {
    if (data.email) { // check if email exists
      const query = {
        text: 'INSERT INTO address (name, ipadress, postalcode) VALUES ($1, $2, $3)',
        values: [data.name, data.ipadress, data.postalcode]
      };

      pool.query(query)
        .then(() => {
          console.log(`Data for ${data.ipadress} inserted successfully`);
        })
        .catch((error) => {
          console.error(`Error inserting data for ${data.ipadress}`, error);
        });
    }
  })
  .on('end', () => {
    console.log('CSV file successfully processed');
  });



//   api for fetching users data from given email
  app.get("/users/:email", async (req, res) => {
    try {
        const { email } = req.params;
      const result = await pool.query('SELECT * FROM users WHERE email = $1', [email]);
      res.send(result.rows);
    } catch (error) {
      console.error(error);
      res.status(500).send("Error retrieving data from database.");
    }
  });

app.listen(3000, () => {
    console.log('Server started');
  });
