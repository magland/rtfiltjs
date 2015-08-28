var fs=require('fs');

var input_path=process.argv[2]||'/home/magland/data/EJ/Spikes_all_channels_filtered.mda';
var output_path=process.argv[3]||'tmp/rtfilt/out.dat';
var num_channels=512;
var chunk_size=1200000;
var num_workers=100;
var num_chunks=1;
var use_srun=1;
if (num_workers>num_channels) num_workers=num_channels;

var JM=new JJobManager();

var X=new RtfiltController();
X.setInputPath(input_path);
X.setOutputPath(output_path);
X.setNumChannels(num_channels);
X.setChunkSize(chunk_size);
X.setNumWorkers(num_workers);
X.setNumChunks(num_chunks);
X.setJobManager(JM);
X.start();

function RtfiltController() {
	this.setInputPath=function(path) {m_input_path=path;}
	this.setOutputPath=function(path) {m_output_path=path;}
	this.setNumChannels=function(num) {opts.num_channels=num;}
	this.setChunkSize=function(num) {opts.chunk_size=num;}
	this.setNumWorkers=function(num) {opts.num_workers=num;}
	this.setNumChunks=function(num) {opts.num_chunks=num;}
	this.setJobManager=function(JM) {m_job_manager=JM;}
	this.start=function() {_start();}

	var m_input_path='',m_output_path='';
	var opts={num_channels:0,chunk_size:100,num_workers:1};
	var m_job_manager=null;

	var m_chunks_to_be_processed=[];
	var m_distributor_job=0;
	var m_worker_jobs=[];
	var m_elapsed_timer=new Date();

	function _start() {
		m_elapsed_timer=new Date();
		mkdir ('tmp/rtfilt');
		mkdir('tmp/rtfilt/step1');
		var JR=new ReaderJob(m_input_path,'tmp/rtfilt/step1/chunk-',opts);
		JR.onChunkRead(function(chunk_path) {
			m_chunks_to_be_processed.push(chunk_path);
		});
		m_job_manager.startJob(JR);
	}
	function all_workers_finished() {
		for (var i=0; i<m_worker_jobs.length; i++) {
			if (!m_worker_jobs[i].isFinished()) return false;
		}
		return true;
	}
	function on_timer() {
		if ((!m_distributor_job)&&(m_chunks_to_be_processed.length>0)&&(all_workers_finished())) {
			m_worker_jobs=[];
			mkdir('tmp/rtfilt/step2');
			var JD=new DistributorJob(m_chunks_to_be_processed[0],'tmp/rtfilt/step2/distributed-',opts);
			JD.distributor_timer=new Date();
			m_chunks_to_be_processed=m_chunks_to_be_processed.slice(1);
			JD.onFinished(function() {
				var elapsed_distributor=(new Date())-JD.distributor_timer;
				var MBs=(opts.chunk_size*num_channels*4/1000000)/(elapsed_distributor/1000);
				console.log('Elapsed time for distributor: '+elapsed_distributor+' #bytes distributed: '+(opts.chunk_size*num_channels*4)+' rate: '+MBs+'MB/s')
				var paths=JD.outputPaths();
				var exec_delay_timer=new Date();
				for (var i=0; i<paths.length; i++) {
					(function() {
						var num_channels0=Math.floor(opts.num_channels*(i+1)/num_workers)-Math.floor(opts.num_channels*i/num_workers);
						var JW=new WorkerJob(paths[i],paths[i]+'.out',opts);
						JW.setNumChannels(num_channels0);
						JW.worker_timer=new Date();
						m_worker_jobs.push(JW);
						m_job_manager.startJob(JW);
						//console.log('Exec delay: '+((new Date())-exec_delay_timer));
						JW.onFinished(function() {
							var elapsed_worker=(new Date())-JW.worker_timer;
							var elapsed_worker2=(new Date())-exec_delay_timer;
							console.log('Elapsed time for worker: '+elapsed_worker+' : '+elapsed_worker2);
						});
					})();
				}
				m_distributor_job=0;
			});
			m_distributor_job=JD;
			m_job_manager.startJob(JD);
		}
		if ((m_chunks_to_be_processed.length==0)&&(m_job_manager.runningJobCount()==0)) {
			var elapsed=(new Date())-m_elapsed_timer;
			console.log ('Done. Elapsed time = '+elapsed);
		}
		else {
			setTimeout(on_timer,10);
		}
	}
	setTimeout(on_timer,1);
}

function write_text_file(path,str) {
	fs.writeFileSync(path,str);
}



function JJobManager() {
	this.startJob=function(job) {m_jobs.push(job); job.onFinished(function() {remove_job(job)}); job.startJob();}
	this.runningJobCount=function() {return m_jobs.length;}

	var m_jobs=[];

	function remove_job(job) {
		for (var i=0; i<m_jobs.length; i++) {
			if (m_jobs[i]==job) {
				m_jobs.splice(i,1);
				return;
			}
		}
	}
}

function JJob(X) {
	X.endJob=function() {m_finished=true; on_finished();}
	X.isFinished=function() {return m_finished;}
	X.onFinished=function(callback) {m_finished_callbacks.push(callback);}

	var m_finished=false;
	var m_finished_callbacks=[];

	function on_finished() {
		for (var i=0; i<m_finished_callbacks.length; i++) {
			(m_finished_callbacks[i])();
		}
	}
}

function WorkerJob(input_path,output_path,opts) {
	var that=this;
	JJob(this);
	this.setNumChannels=function(num) {m_num_channels=num;}
	this.startJob=function() {_startJob();}

	var m_output_paths=[];
	var m_num_channels=0;

	function _startJob() {
		var exe=__dirname+'/bin/rtfilt';
		var args=['filter',input_path,output_path,m_num_channels,opts.chunk_size];
		exec_process(exe,args,{srun:use_srun},function(a) {
			that.endJob();
		});
	}
}

function DistributorJob(input_path,output_prefix,opts) {
	var that=this;
	JJob(this);
	this.startJob=function() {_startJob();}
	this.outputPaths=function() {return m_output_paths;}

	var m_output_paths=[];

	function _startJob() {
		m_output_paths=[];
		for (var i=0; i<opts.num_workers; i++) {
			m_output_paths.push(output_prefix+i+'.dat');
		}
		var exe=__dirname+'/bin/rtfilt';
		var args=['distribute',input_path,output_prefix,opts.num_channels,opts.chunk_size,opts.num_workers];
		exec_process(exe,args,{srun:use_srun},function(a,b,c) {
			that.endJob();
		});
	}
}



function ReaderJob(input_path,output_prefix,opts) {
	var that=this;

	JJob(this);
	this.onChunkRead=function(callback) {m_chunk_read_callbacks.push(callback);}
	this.startJob=function() {_startJob();}

	var m_chunk_read_callbacks=[];
	var m_extra_data=new Buffer(0);
	var m_bytes_per_chunk=opts.num_channels*opts.chunk_size*4;

	function _startJob() {
		var current_chunk_number=0;
		var current_chunk_path=output_prefix+current_chunk_number+'.dat';
		var current_num_bytes_written=0;
		var current_WS=fs.createWriteStream(current_chunk_path);

		var reader_timer=new Date();
		var reader_bytes=0;
		var SS=fs.createReadStream(input_path);
		SS.on('data',function(dd) {
			reader_bytes+=dd.length;
			if (that.isFinished()) return;
			while (dd.length>0) {
				if (current_num_bytes_written+dd.length<m_bytes_per_chunk) {
					current_WS.write(dd);
					current_num_bytes_written+=dd.length;
					dd=new Buffer(0);
				}
				else {
					var ind=m_bytes_per_chunk-current_num_bytes_written;
					current_WS.write(dd.slice(0,ind));
					dd=dd.slice(ind);
					current_WS.end();
					current_chunk_number++;
					on_chunk_read(current_chunk_path);
					if (current_chunk_number>=opts.num_chunks) {
						reader_timer_elapsed=(new Date())-reader_timer;
						console.log('Elapsed time for reader: '+reader_timer_elapsed+' bytes read: '+reader_bytes+' mb/sec: '
							+((reader_bytes/1000000)/(reader_timer_elapsed/1000)));

						dd=new Buffer(0);
						SS.destroy();
						that.endJob();
					}
					else {
						current_chunk_path=output_prefix+current_chunk_number+'.dat';
						current_num_bytes_written=0;
						current_WS=fs.createWriteStream(current_chunk_path);
					}
				}
			}
		});
		SS.on('end',function() {
			//current_WS.end();
			//this.endJob();
		});
	}
	function on_chunk_read(path) {
		for (var i=0; i<m_chunk_read_callbacks.length; i++) {
			(m_chunk_read_callbacks[i])(path);
		}
	}
}

function exec_process(exe,args,opts,callback) {
	if (opts.srun) {
		var cmd='srun'
		args.splice(0,0,exe);
	}
	else {
		cmd=exe;
	}
	var cmdstr=cmd;
	for (var i=0; i<args.length; i++) cmdstr+=' '+args[i];
	console.log('Executing: '+cmdstr);
	
	var spawn = require('child_process').spawn;
    var XX = spawn(cmd,args);
    XX.stdout.on('data', function(data) {
         console.log(data.toString());
    });
    XX.stderr.on('data', function(data) {
         console.log(data.toString());
    });
    XX.on('close', function(code) {
        return callback(code);
    });
	
}

/*function exec_process(exe,args,callback) {
	var cmd='srun '+exe;
	for (var i=0; i<args.length; i++) {
		cmd+=' '+args[i];
	}
	console.log ('executing: '+cmd);
	require('child_process').exec(cmd,function(err,output) {
		if (output) console.log(output);
		callback(err,output);
	});
}*/

function mkdir(path) {
	try {
		fs.mkdirSync(path);
	}
	catch(err) {

	}
}
