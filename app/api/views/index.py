from data_engineering.common.sso.token import login_required
from flask import render_template


@login_required
def index():
    return render_template('data_store_service/index.html', active_menu='home')
