#!/usr/bin/env python
# -*- coding: utf-8 -*-
import traceback
from master import DashboardMaster
from mailer import Mailer
from mailer import Message
from config import MAILER_HOST, MAILER_TO, MAILER_FROM, MAILER_SUBJECT

if __name__ == '__main__':
	try:
		dashboard_master = DashboardMaster()
		dashboard_master.run()
	except Exception, e:
		print "Exception caught, details and the traceback will be sent to %s" % MAILER_TO
		message = Message(From=MAILER_FROM,
		                  To=MAILER_TO,
		                  charset="utf-8")
		message.Subject = MAILER_SUBJECT
		message.Body 		= traceback.format_exc()

		sender = Mailer(MAILER_HOST)
		sender.send(message)
	finally:
		print "Finished"
