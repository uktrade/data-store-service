from flask import render_template


def index():
    return render_template('data_store_service/index.html', active_menu='home')
