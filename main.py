from flask import Flask, send_file, render_template, redirect, request, url_for
import secrets
from connect import query_reservas

app = Flask(__name__)


@app.route('/')
def home():
    return render_template("index.html")


@app.route('/reservas')
def get_reservas():
    if request.form.get('reservas_key') == secrets.RESERVAS_KEY:

        print('making excel')
        print('returning file')
        print('redirect to home')
    else:
        return jsonify({"error": "Sorry, that's not allowed. Make sure you have the correct key"}), 403


if __name__ == "__main__":
    app.run(debug=True)