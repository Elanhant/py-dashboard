#!/usr/bin/env python
# -*- coding: utf-8 -*-
import traceback, os, sys
import smtplib
from master import DashboardMaster
from config import MAILER_HOST, MAILER_TO, MAILER_FROM, MAILER_SUBJECT, LOG_FILE_NAME

if __name__ == '__main__':
	log_path = os.path.join(sys.path[0], LOG_FILE_NAME)
	f = open(log_path, 'w+')
	f.write('Starting dashboard master...   ')
	try:
		dashboard_master = DashboardMaster(log=f)
		dashboard_master.run()
	except Exception, e:
		# Сообщаем об ошибке
		exc_msg = "Exception caught, details and the traceback will be sent to %s" % MAILER_TO
		print exc_msg
		f.write(exc_msg)

		# Составляем тело e-mail
		msg = ("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n"
       % (MAILER_FROM, ", ".join(MAILER_TO), MAILER_SUBJECT))
		divider = '\n' + '='*5 + ' RUNTIME LOG ' + '='*5 + '\n'

		# Сбрасываем указатель на начало лог-файла
		f.seek(0)
		msg = msg + traceback.format_exc() + divider + f.read()

		# Отправляем e-mail
		server = smtplib.SMTP(MAILER_HOST)
		server.sendmail(MAILER_FROM, MAILER_TO, msg)
	finally:
		f.close()
		print "Finished"
