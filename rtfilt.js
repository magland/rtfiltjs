var fs=require('fs');

var input_path=process.argv[2]||'/home/magland/data/EJ/Spikes_all_channels_filtered.mda';
var output_path=process.argv[3]||'/tmp/rtfilt/out.dat';
var num_channels=512;
var chunk_size=100;
var num_workers=10;

var JM=new JJobManager();

var X=new RtfiltController();
X.setInputPath(input_path);
X.setOutputPath(output_path);
X.setNumChannels(num_channels);
X.setChunkSize(chunk_size);
X.setNumWorkers(num_workers);
X.setJobManager(JM);
X.start();

function RtfiltController() {
	this.setInputPath=function(path) {m_input_path=path;}
	this.setOutputPath=function(path) {m_output_path=path;}
	this.setNumChannels=function(num) {opts.num_channels=num;}
	this.setChunkSize=function(num) {opts.chunk_size=num;}
	this.setNumWorkers=function(num) {opts.num_workers=num;}
	this.setJobManager=function(JM) {m_job_manager=JM;}
	this.start=function() {_start();}

	var m_input_path='',m_output_path='';
	var opts={num_channels:0,chunk_size:100,num_workers:1};
	var m_job_manager=null;

	var m_chunks_to_be_processed=[];
	var m_distributor_job=0;
	var m_worker_jobs=[];

	function _start() {
		mkdir('/tmp/rtfilt/step1');
		var JR=new ReaderJob(m_input_path,'/tmp/rtfilt/step1',opts);
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
		console.log('timer');
		if ((!m_distributor_job)&&(m_chunks_to_be_processed.length>0)&&(all_workers_finished())) {
			m_worker_jobs=[];
			mkdir('/tmp/rtfilt/step2');
			var JD=new DistributorJob(m_chunks_to_be_processed[0],'/tmp/rtfilt/step2',opts);
			m_chunks_to_be_processed=m_chunks_to_be_processed.slice(1);
			JD.onFinished(function() {
				console.log('JD finished');
				var paths=JD.outputPaths();
				for (var i=0; i<paths.length; i++) {
					var JW=new WorkerJob(paths[i],paths[i]+'.out',opts);
					m_worker_jobs.push(JW);
					m_job_manager.startJob(JW);
				}
				m_distributor_job=0;
			});
			m_distributor_job=JD;
			m_job_manager.startJob(JD);
		}
		console.log(m_chunks_to_be_processed.length);
		console.log(m_job_manager.runningJobCount());
		if ((m_chunks_to_be_processed.length==0)&&(m_job_manager.runningJobCount()==0)) {
			console.log ('Done.');
		}
		else {
			setTimeout(on_timer,1000);
		}
	}
	setTimeout(on_timer,1000);
	function mkdir(path) {
		try {
			fs.mkdirSync(path);
		}
		catch(err) {

		}
	}
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
	this.startJob=function() {_startJob();}

	var m_output_paths=[];

	function _startJob() {
		var exe='/home/magland/dev/rtfilt/bin/rtfilt';
		var args=['filter',input_path,output_path,opts.num_channels,opts.chunk_size];
		exec_process(exe,args,function() {
			that.endJob();
		});
	}
}

function DistributorJob(input_path,output_dir,opts) {
	var that=this;
	JJob(this);
	this.startJob=function() {_startJob();}
	this.outputPaths=function() {return m_output_paths;}

	var m_output_paths=[];

	function _startJob() {
		m_output_paths=[];
		for (var i=0; i<opts.num_workers; i++) {
			m_output_paths.push(output_dir+'/data-'+i+'.dat');
		}
		var exe='/home/magland/dev/rtfilt/bin/rtfilt';
		var args=['distribute',input_path,output_dir,opts.num_channels,opts.chunk_size,opts.num_workers];
		exec_process(exe,args,function(a,b,c) {
			console.log(a);
			console.log(b);
			console.log(c);
			that.endJob();
		});
	}
}

function exec_process(exe,args,callback) {
	var cmd=exe;
	for (var i=0; i<args.length; i++) {
		cmd+=' '+args[i];
	}
	require('child_process').exec(cmd,callback);
}

function ReaderJob(input_path,output_dir,opts) {
	var that=this;

	JJob(this);
	this.onChunkRead=function(callback) {m_chunk_read_callbacks.push(callback);}
	this.startJob=function() {_startJob();}

	var m_chunk_read_callbacks=[];
	var m_extra_data=new Buffer(0);
	var m_bytes_per_chunk=opts.num_channels*opts.chunk_size*4;

	function _startJob() {
		var current_chunk_number=0;
		var current_chunk_path=output_dir+'/chunk-'+current_chunk_number+'.dat';
		var current_num_bytes_written=0;
		var current_WS=fs.createWriteStream(current_chunk_path);

		var SS=fs.createReadStream(input_path);
		SS.on('data',function(dd) {
			if (that.isFinished()) return;
			while (dd.length>0) {
				if (current_num_bytes_written+dd.length<m_bytes_per_chunk) {
					current_WS.write(dd);
					current_num_bytes_written+=dd.length;
					dd=new Buffer(0);
					console.log('test 1');
				}
				else {
					console.log('test 2');
					var ind=m_bytes_per_chunk-current_num_bytes_written;
					current_WS.write(dd.slice(0,ind));
					dd=dd.slice(ind);
					current_WS.end();
					current_chunk_number++;
					on_chunk_read(current_chunk_path);
					if (current_chunk_number>=5) {
						console.log('test 3 '+m_bytes_per_chunk);
						dd=new Buffer(0);
						SS.destroy();
						that.endJob();
					}
					else {
						console.log('current_chunk_number = '+current_chunk_number);
						current_chunk_path=output_dir+'/chunk-'+current_chunk_number+'.dat';
						current_num_bytes_written=0;
						console.log(current_chunk_path);
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
