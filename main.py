#!/usr/bin/env python
# -*- coding: utf-8 -*-
import traceback
import smtplib
from master import DashboardMaster
from config import MAILER_HOST, MAILER_TO, MAILER_FROM, MAILER_SUBJECT

if __name__ == '__main__':
	try:
		dashboard_master = DashboardMaster()
		dashboard_master.run()
	except Exception, e:
		print "Exception caught, details and the traceback will be sent to %s" % MAILER_TO
		msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n"
       % (MAILER_FROM, ", ".join(MAILER_TO), MAILER_SUBJECT))
		msg = msg + traceback.format_exc()

		server = smtplib.SMTP(MAILER_HOST)
		server.sendmail(MAILER_FROM, MAILER_TO, msg)
	finally:
		print "Finished"
