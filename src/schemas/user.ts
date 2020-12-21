import { Schema } from 'mongoose';

export default new Schema({
  first_name: String,
  last_name: String,
  email: String,
  books: [{
    name: String,
    isbn: String
  }]
});
