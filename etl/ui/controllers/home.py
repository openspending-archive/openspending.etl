from openspending.etl.ui.lib.base import BaseController, render

class HomeController(BaseController):
    def index(self):
        return render('home/index.html')

