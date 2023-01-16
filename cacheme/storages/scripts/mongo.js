db = connect('mongodb://localhost/myDatabase');

db.cacheme_data.create_index('key', (unique = True));
db.cacheme_data.create_index('expire');
