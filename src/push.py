#!/usr/bin/python
# -*- coding: utf-8 -*-

from gevent import monkey
monkey.patch_all()

import json
import logging
import Queue
import select
import socket
import ssl
import time
from threading import Thread

import beanstalkc

import apns
import config


class Pipe(object):
    def __init__(
            self, beanstalkd_host, beanstalkd_port, tube,
            gateway_host, gateway_port, key_file, cert_file, master_worker):
        self.beanstalkd_host = beanstalkd_host
        self.beanstalkd_port = beanstalkd_port
        self.tube = tube
        self.gateway_host = gateway_host
        self.gateway_port = gateway_port
        self.key_file = key_file
        self.cert_file = cert_file
        self.master_worker = master_worker

        self.push_id = 0
        self.last_push_time = 0
        self.pushed_buffer = Queue.Queue(maxsize=1000)
        self.beanstalk = None
        self.gateway_connection = None
        self.gateway_invalid = False

    def init_beanstalk(self):
        # init beanstalk
        logging.debug('Init beanstalk start')
        while True:
            try:
                if self.beanstalk:
                    self.beanstalk.close()

                self.beanstalk = beanstalkc.Connection(
                    self.beanstalkd_host, self.beanstalkd_port)
                logging.debug(
                    'Connect to %s:%s success' % (
                        self.beanstalkd_host, self.beanstalkd_port))
                self.beanstalk.watch(self.tube)
                for tube in self.beanstalk.watching():
                    if tube != self.tube:
                        self.beanstalk.ignore(tube)
                self.beanstalk.use(tube)
                logging.debug('Init beanstalk end')
                return
            except beanstalkc.SocketError:
                logging.debug(
                    'Connect to %s:%s failed' % (
                        self.beanstalkd_host, self.beanstalkd_port))
                time.sleep(2)
                continue
            except Exception as e:
                logging.critical('Unknown init beanstalk error: %s' % e)

    def _delete_old_jobs(self):
        for i in range(1000):
            job = self.beanstalk.peek_ready()
            if job.stats()['age'] > 3600*4:
                logging.debug(
                    'After init_gateway failed, Deleting too old job: %s' %
                    job.body)
                job.delete()
            else:
                break

    def init_gateway(self):
        logging.debug('Init gateway start')
        while True:
            try:
                if not self.gateway_connection:
                    self.gateway_connection = apns.GatewayConnection(
                        host=self.gateway_host,
                        port=self.gateway_port,
                        cert_file=self.cert_file,
                        key_file=self.key_file,
                    )
                else:
                    self.gateway_connection.reconnect()
                logging.debug('Init gateway end')
                return
            except ssl.SSLError as e:
                logging.error('Init gateway error: %s' % e)
                if e.errno == ssl.SSL_ERROR_SSL:
                    self.gateway_invalid = True
                    time.sleep(3600)
                    logging.debug('Invalid key')
            except (socket.error, IOError) as e:
                logging.debug('Gateway connect error %s' % e)
            self._delete_old_jobs()
            time.sleep(2)

    def process_gateway_input(self):
        buff = self.gateway_connection.read(apns.ERROR_RESPONSE_LENGTH)
        if len(buff) == apns.ERROR_RESPONSE_LENGTH:
            command, status, error_identifier = \
                apns.unpack(apns.ERROR_RESPONSE_FORMAT, buff)

            if 8 == command:
                found = False
                while not self.pushed_buffer.empty():
                    identifier, job = self.pushed_buffer.get()
                    if found:
                        logging.debug('Reput failed job %s' % identifier)
                        self.beanstalk.put(json.dumps(job))
                    elif identifier == error_identifier:
                        logging.debug('Found error identifier %s' % identifier)
                        found = True
        elif len(buff) == 0:
            logging.debug('Close by server')
        else:
            logging.debug('Unexcepted read buf size %s' % len(buff))
        logging.debug('Process gateway input end')

    def push_job(self):
        job = self.beanstalk.reserve(timeout=10)
        if not job:
            logging.debug('No job found')
            return

        # delete job that job age > 3 hours
        if job.stats()['age'] > 10800:
            logging.debug('Reserved too old job: %s' % job.body)
            job.delete()
            return
        logging.debug('Reserved job: %s' % job.body)

        try:
            job_body = json.loads(job.body)
        except ValueError:
            logging.debug(
                'Failed to loads job body: %s' % job.body)
            job.bury()

        # push job
        self.push_id += 1
        try:
            logging.debug('Send notification: %s %s' % (self.push_id, job.body))
            expire_seconds = job_body.get(
                'expire_seconds', config.EXPIRE_SECONDS)
            expiry = int(time.time()) + expire_seconds
            self.gateway_connection.send_notification(
                job_body['device_token'],
                apns.Payload(**job_body['payload']),
                self.push_id,
                expiry)
        except apns.InvalidTokenError:
            pass
        except Exception as e:
            logging.debug('Unknown send notification error: %s' % e)
            job.release()
            raise
        if self.pushed_buffer.full():
            self.pushed_buffer.get()
        logging.debug('Enqueue pushed buffer: %s' % self.push_id)
        self.pushed_buffer.put((self.push_id, job_body))
        self.last_push_time = time.time()

        logging.debug('Delete job: %s %s' % (self.push_id, job.body))
        job.delete()

    def reserve_and_push(self):
        logging.debug('Reserve and push start')
        while True:
            rlist, wlist, _ = select.select(
                [self.gateway_connection.connection()],
                [self.gateway_connection.connection()],
                [],
                10)
            if rlist:
                logging.debug('Start reading from gateway')
                self.process_gateway_input()
                self.gateway_connection.reconnect()
            elif wlist:
                logging.debug('Start writing to gateway')
                self.push_job()

            if self.ok_to_stop():
                break

    def need_to_start(self):
        if self.master_worker:
            return True
        tube_stat = self.beanstalk.stats_tube(self.tube)
        if tube_stat['current-jobs-ready'] > 100:
            return True
        return False

    def ok_to_stop(self):
        if self.master_worker:
            return False
        if time.time() - self.last_push_time > 10:
            return True
        return False

    def run(self):
        self.init_beanstalk()

        while True:
            try:
                if not self.need_to_start():
                    logging.debug('Sleepy')
                    time.sleep(30)
                    continue
                logging.debug('Start to reserve and push')
                self.init_gateway()
                self.reserve_and_push()
                self.gateway_connection.disconnect()
                logging.debug('Stop to reserve and push')
            except beanstalkc.SocketError as e:
                logging.error('Beanstalkd connection error: %s' % e)
                self.init_beanstalk()
            except (ssl.SSLError, socket.error, IOError) as e:
                logging.error('Apns connection error: %s' % e)
            except Exception as e:
                logging.critical('Unknown error: %s' % e)


if __name__ == '__main__':
    logging.basicConfig(
        format=config.LOGGING_FORMAT, level=config.LOGGING_LEVEL)
    for app_name, app_config in config.APPS.items():
        for i in range(app_config[2]):
            pipe = Pipe(
                config.BEANSTALKD_HOST, config.BEANSTALKD_PORT,
                config.PUSH_TUBE % app_name, config.APNS_HOST,
                config.APNS_PORT, app_config[1], app_config[0], i == 0)
            t = Thread(target=pipe.run, name='%s.%d' % (app_name, i))
            t.start()
