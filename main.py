from flask import Flask, send_file, render_template, redirect, request, url_for
import psycopg2

app = Flask(__name__)
conn = psycopg2.connect(host="localhost", database="cyrela", user="postgres", password="1234143213")

@app.route('/', methods=["GET", "POST"])
def home():
    if request.method == "POST":
        file = get_reservas()
        return redirect(url_for('home'))
    else:
        return render_template("index.html")


def get_reservas():
    print('querying')
    print('making excel')
    #query no postgres
    #transform in excel


if __name__ == "__main__":
    app.run(debug=True)