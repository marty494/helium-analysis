from flask import Flask
from flask import Flask, render_template

server = Flask(__name__)

@server.route('/')
def home():
    return render_template('home.html')

@server.route('/about')
def about():
    return render_template('about.html')

if __name__ == '__main__':
    server.run(host='0.0.0.0')
