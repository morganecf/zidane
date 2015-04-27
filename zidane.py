'''
This script allows me to start a python scraper from src/ given input
data and output directory in data/ in the background on multiple servers. 
It also stores a log file in data/logfiles titled by the name of the script 
and the timestamp. You can specify if you want to be notified if the scraper 
has terminated, and you can check on the status of the scraper. 

The JSON config file takes 6 parameters. 

script: 		The name of the script to be run, should be located in src. 
servers: 		A list of the servers. The same server can appear twice, 
		     	and two version of the same job will get run on it.
parameters: 	Should be a dict. The keys should contain the names of the script's 
				arguments (if using something like argparse) or the index of their positions 
				(if using something like sys). The values should be lists of the argument 
				values, corresponding positionally to which server you want this input 
				to be run on. 
notify: 		Set to true if you want to get notified if a job is done. 
log-override:	Set this to a path to override the log file path, which is at the root 
				dir under logfiles/. 
data-root: 		Optional. Takes the root path where all the input/output data is stored.
				That way you don't have to write it out each time. If you want to prepend
				this path to only some of the values, place a caret (^) in front of the 
				values that don't take the path (ex: numerical values). 

Usage:
python zidane.py --conf test.json    						// Run a scraper on multiple servers
python zidane.py --scraper crawler.py 						// Get crawler.py's status across all servers
python zidane.py --host serenity							// Get status of all crawlers on serenity 
python zidane.py --scraper crawler.py --host serenity		// Get crawler.py's status on serenity
python zidane.py --scraper crawler.py --kill 1 				// Kill all of these jobs 

Named after Zidane because he was a legendary center-mid distributor. 
'''

import os
import json
import argparse as ap
import subprocess
from datetime import datetime

# NOTE: All paths are relative to mciot/reddit-topic-modeler
src_root = 'src'
log_path = 'logfiles'
hosts = ['epiphyte', 'enterprise', 'serenity']

class ConfError(Exception):
	pass 

def optparser(script, parameters, data_path):
	commands = []
	num_to_run = len(parameters[parameters.keys()[0]])
	for i in range(num_to_run):
		command = script 
		for opt, vals in parameters.iteritems():
			if not vals[i].startswith('^'):
				val = os.path.join(data_path, vals[i])
			else:
				val = vals[i].replace('^', '')
			command += ' ' + opt + ' ' + val
		commands.append(command)
	return commands

def argparser(script, parameters, data_path):
	commands = []
	num_to_run = len(parameters[parameters.keys()[0]])
	for i in range(num_to_run):
		command = script 
		for p in range(len(parameters)):
			val = parameters[str(p + 1)][i]
			if not val.startswith('^'):
				val = os.path.join(data_path, val)
			else:
				val = val.replace('^', '')
			command += ' ' + val
		commands.append(command)
	return commands

def hasOptions(keys):
	for k in keys:
		try:
			k = int(k)
		except ValueError:
			return True 
	return False 

def now():
	time = datetime.today()
	timestamp = ':'.join([str(time.hour), str(time.minute), str(time.second)])
	return timestamp


# NOTE: if script == '', will just find all python processes
# this is probably fine for now but should be adjusted 
# ex: ps -ax | grep -i 'python fetch-link-content.py'
def get_jobs(script, server):
	ssh = "ssh -f mciot@" + server + ".cs.mcgill.ca "
	if script:
		ps = "ps ax | grep -i 'python " + script + "'"
	else:
		ps = "ps ax | grep -i mciot"
	ps = '"' + ps + '"'
	processes = os.popen(ssh + ps).read().splitlines()
	jobs = []
	for process in processes:
		# Skip over the grep job and the ssh job - redundant
		if 'grep -i' in process or 'sh -c' in process:
			continue
		pid = process.split()[0]
		command = 'python ' + process.split('python')[1]
		jobs.append((pid, command))
	return jobs

# ssh -f mciot@serenity.cs.mcgill.ca "sh -c '. ./reddit ; cd src; nohup python fetch-link-content.py >> test.txt 2>> errtest.txt; echo done &'"
# commands = ['ssh', 'mciot@serenity.cs.mcgill.ca', 'sh -c', 
# '. ./reddit; cd src; nohup python fetch-link-content.py >> ../logfiles/fetch-link-content@serenity@2015-15-04_20:16.log 
# 		2>> ../logfiles/fetch-link-content@serenity@2015-15-04_20:16.log; echo done &']
def run_job(command, server, log_dir, notify):
	# Create logfile name 
	script = command.split('.py')[0]
	timestamp = datetime.today().strftime('%Y-%d-%m_%H:%M')
	logfile_name = script + '@' + server + '@' + timestamp + '.log'
	logfile = os.path.join('..', log_dir, logfile_name)

	# List commands 
	commands = ['ssh', '-f', 'mciot@' + server + '.cs.mcgill.ca', 'sh -c']
	actions = ['. ./reddit', 'cd src']
	actions.append('nohup python ' + command + ' >> ' + logfile + ' 2>> ' + logfile)
	if notify:
		notification = 'python notify.py morganeciot@gmail.com ' + script + ' ' + server
		actions.append(notification)
	actions[-1] += ' &'
	actions = "'" + ';'.join(actions) + "'"
	commands.append(actions)

	# Create process object so that can access pid 
	process = subprocess.Popen(commands)
	process.wait()
	return process 

def distribute(conf):
	# Load the configuration
	conf = json.load(open(conf))
	script = conf.get('script')
	servers = conf.get('servers')
	data_root = conf.get('data-root')
	parameters = conf.get('parameters')
	notify = conf.get('notify')
	log_override = conf.get('log-override')

	# TODO: Validate conf file -- would need to do this on server
	# if not (script and script.endswith('.py')):
	# 	raise ConfError('conf needs a python script')
	# if not (servers and type(servers) == list):
	# 	raise ConfError('conf needs a list of servers')
	# if data_root is not None and not os.path.exists(data_root):
	# 	raise ConfError('root data path does not exist')
	# if log_override and not os.path.exists(log_override):
	# 	raise ConfError('the log override dir does not exist')

	log_dir = log_override or log_path

	# This will save all jobs run on this day, for future reference
	jobfilename = os.path.join('jobs', 'jobs' + '@' + datetime.today().strftime('%Y-%d-%m'))
	jobfile = open(jobfilename, 'a')

	# If the script doesn't take any parameters, just run it 
	if len(parameters) == 0 or any(filter(lambda k: len(parameters[k]) == 0, parameters.keys())):
		print 'Running script without parameters.'
		for server in servers:
			job = run_job(script, server, log_dir, notify)
			print '\nLaunched:', script, 'on', server, '\t', job.pid
			jobinfo = '\t'.join([now(), str(job.pid), script, server])
			jobfile.write(jobinfo + '\n')

	# Otherwise construct commands to run
	else:
		# We'll be in src 
		data_root = data_root or ''
		data_root = os.path.join('..', 'data', data_root) 
 
		if hasOptions(parameters.keys()):
			commands = optparser(script, parameters, data_root)
		else:
			commands = argparser(script, parameters, data_root)

		# Run script on all the servers with its commands 
		for i, server in enumerate(servers):
			job = run_job(commands[i], server, log_dir, notify)
			print '\nLaunched:', commands[i], 'on', server, '\t', job.pid
			jobinfo = '\t'.join([now(), str(job.pid), script, server])
			jobfile.write(jobinfo + '\n')

	# Done! 
	print '\nDone launching jobs!'
	jobfile.close()

def kill_job(scriptname, server):
	jobs = get_jobs(scriptname, server)
	if len(jobs) == 0:
		print 'There are no jobs to kill on', server, 'matching', scriptname
	else:
		ssh = ['ssh', '-f', 'mciot@' + server + '.cs.mcgill.ca']
		for pid, command in jobs:
			killer = subprocess.Popen(ssh[:] + ['kill ' + str(pid)])
			killer.wait()
			print 'Killed', pid

def kill(scriptname, server):
	if server:
		kill_job(scriptname, server)
	else:
		for host in hosts:
			kill_job(scriptname, host)

def status(scriptname, server):
	jobs = get_jobs(scriptname, server) 
	if len(jobs) == 0:
		print scriptname, 'is currently not running on', server
	elif len(jobs) == 1:
		print '\nFound 1 job on', server, ':'
	else:
		print '\nFound', len(jobs), 'jobs on', server, ':'
	for pid, command in jobs:
		print pid, '\t', command


parser = ap.ArgumentParser(description="Distribute some scrapers.")
parser.add_argument('--conf', help='the config file for distributing the scrapers')
parser.add_argument('--script', help='to get status of this script')
parser.add_argument('--host', help='to get status of script on this server')
parser.add_argument('--kill', help='will kill the script provided on ALL hosts, unless a host is specified')
params = parser.parse_args()

# If a conf file was specified, load and run it 
if params.conf:
	distribute(params.conf)

# If only script was specified, get info for this script across all servers
if params.script and not params.host:
	for host in hosts:
		status(params.script, host)

# If only the server was specified, get info for everything I'm running on this server
elif params.host and not params.script:
	status('', params.host)

# If both were specified, get info for scripts of this name running on this server
elif params.host and params.script:
	status(params.script, params.host)

# If kill was specified, kill the pids (ignore if conf was also specified)
if params.kill and not params.conf:
	kill(params.kill, params.host)
	



