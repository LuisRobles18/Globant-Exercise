from app import app
from flaskext.mysql import MySQL

mysql = MySQL()

#MySQL configuration parameters
app.config['MYSQL_DATABASE_USER'] = 'luisrobl_globant'
app.config['MYSQL_DATABASE_PASSWORD'] = 'e42w0}d1t(T.'
app.config['MYSQL_DATABASE_DB'] = 'luisrobl_globant'
app.config['MYSQL_DATABASE_HOST'] = 'luisrobles.me'

mysql.init_app(app)