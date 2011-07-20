from pylons import url
from pylons.controllers.util import redirect

from openspending.etl.ui.lib.base import BaseController

class HomeController(BaseController):
    def index(self):
        return redirect(url(controller='sources', action='index'))
