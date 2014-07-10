var MongoServer = require("mongo-sync").Server;
var fs = require('node-fs');
var fse = require('fs-extra');
var util = require('util');
var osSep = process.platform === 'win32' ? '\\' : '/';
var moment = require('moment');

var argv = require('yargs')
	.usage('Usage: $0 -s <source> -t <target> [-d <mongodb_name>]')
	.demand(['s','t'])
	.default('d','file_transfer')
	.default('v',true)
  .boolean('v')
	.alias('s','source')
	.alias('d','database')
	.alias('t','target')
  .alias('q','exclude_file_types')
  .alias('v','validate_copy')
	.describe('d','mongodb name to track this copy')
	.describe('s','source directory or file')
	.describe('t','target directory or file')
  .describe('q','comma separated list of file extensions to exclude from transfer')
  .describe('v','validate the target after the file as been copied')
	.check(checkArgv)
	.argv
;

var server = new MongoServer('127.0.0.1')
var transfer_db = server.db(argv.d);
var transfer_log = transfer_db.getCollection("transfer_log");
var transfer_metadata = transfer_db.getCollection("transfer_metadata");
var transfer_files = transfer_db.getCollection("transfer_files");
var directories_to_list = transfer_db.getCollection("directories_to_list");
var excluded_file_types = null;
var validate_copy = argv.v;
if(argv.q) { excluded_file_types = argv.q.split(","); }

//Ensure source/target end with osSep;
if(!strEndsWith(argv.source, osSep)){
  argv.source += osSep;
}
if(!strEndsWith(argv.target, osSep)){
  argv.target += osSep;
}

var metadata = transfer_metadata.findOne();
if(metadata != null){
  console.log("Previous metadata entries, resuming transfer.");
  console.log(transfer_metadata.findOne());
  if(metadata.source != argv.source || metadata.target != argv.target){
	console.log("Source//Target directories differ from previous run.  Refusing to start.");
	console.log("Metadata Source//Target    : "+metadata.source+"//"+metadata.target);
	console.log("Command Line Source//Target: "+argv.source+"//"+argv.target);
	process.exit(1);
  }
} else {
  console.log("First time this transfer has been run");

  transfer_metadata.update({"_id":1},{$set:{"transfer_started":true, "status":"starting","source":argv.source,"target":argv.target}},{"upsert":true});
  metadata = {"_id":1,"transfer_started":true, "status":"starting","source":argv.source,"target":argv.target};
}

if(metadata['status'] == "starting"){
// Just starting out.  Add source directory to directories_to_list
  console.log("Adding source directory items to list of items to pull");
  
  try{
	var files = fs.readdirSync(metadata.source);
	for(var i = 0; i < files.length; i++){
	  directories_to_list.insert({"_id":files[i]});
	}
  } catch(ex) {
	console.log("Could not stat source: "+JSON.stringify(ex));
	process.exit(2);
  }
  transfer_metadata.update({"_id":1},{$set:{"status":"listing_files"}},{"upsert":true});
  metadata.status = "listing_files";
} else {
  console.log("Not just starting out...");
}

if(metadata.status == "listing_files"){
  console.log("At step: listing_files");
  var dirs = directories_to_list.find().toArray();
  for(var i = 0; i < dirs.length; i++){
	addSubdirsAndFilesRemoveDir(dirs[i]._id);
  }
  transfer_metadata.update({"_id":1},{$set:{"status":"copying_files"}},{"upsert":true});
  metadata.status = "copying_files";
} else {
  console.log("Not listing files...");
}

var files_processed;
var durations = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0];
var start_time = new Date();
if(metadata.status == "copying_files"){
  var max_callbacks = 1000;
  var open_callbacks = 0;
  var files_processed = 0 - max_callbacks;
  console.log("Remaining files: "+transfer_files.find().count());
  startFileCopy(max_callbacks);
} else {
  console.log("Not copying files...");
}

//Done

//////////////////////////
// Function Definitions //
//////////////////////////

function startFileCopy(batchSize){
  var numCompleted = 0;
  durations.push(new Date());
  var twentyAgo = durations.shift();
  var numRemaining = transfer_files.find().count();
  var recent_rate = (batchSize * 20 / ((new Date() - twentyAgo)/1000)).toFixed(2);
  var seconds_remaining = (numRemaining / recent_rate).toFixed(0);
  console.log("Remaining files: "+numRemaining+", current rate: "+recent_rate+", ETC: "+moment().add('seconds',seconds_remaining).calendar());
  var files = transfer_files.find().limit(batchSize).toArray();
  if(files){
    for(var i = 0; i < files.length; i++){
	  if(files[i] !== null){
		  if(i == 0) {console.log("Copying: "+JSON.stringify(files[i]));}
		  copyFile(metadata.source + files[i]._id,metadata.target + files[i]._id, files[i]._id, function(err, fileId){
			if(err){
			  console.log("Failure!");
			  console.log(err);
			  console.log("Failure FROM: "+metadata.source + fileId);
			  console.log("Failure   TO: "+metadata.target + fileId);
              transfer_log.insert({"log_message":"failure to copy file","related_file":fileId, "date":new Date()});
              transfer_files.remove({"_id":fileId});
			} else {
			  //console.log("Success!");
			  //console.log("Copied FROM: "+metadata.source + fileId);
			  //console.log("Copied   TO: "+metadata.target + fileId);
			  try{
			    transfer_files.remove({"_id":fileId});
			  } catch(ex){
			    console.log("Failed to remove: "+fileId);
				console.log(ex);
			  }
			}
			numCompleted++;
			if(numCompleted >= batchSize){
			  startFileCopy(batchSize);
			} else if (numCompleted >= numRemaining){
			  console.log("Done everything!");
			  server.close();
			}
		  });
	  } else {
	    console.log("No more files in queue");
	  }
    }
  }
}

function addSubdirsAndFilesRemoveDir(dir){ 
  console.log("#addSubdirsAndFilesRemoveDir: "+dir);
  try{ 
    try{
      var stat = fs.statSync(metadata.source + dir);
    } catch (ex) {
      //could not stat file
      console.log(ex);
      console.log("Could not stat, removing from list: "+JSON.stringify(metadata.source + dir));
      transfer_log.insert({"log_message":"Could not stat file","related_file":dir, "full_path":metadata.source + dir,"date":new Date()});
      directories_to_list.remove({"_id":dir});
    }
    if(stat){
      if(stat.isFile()){
        //File, need to add file to copy list, remove from directory list
        transfer_files.insert({"_id":dir});
        directories_to_list.remove({"_id":dir});
      } else if(stat.isDirectory()) {
        //Directory.  Add subdirectories to directory list, remove this directory, call subdirectories
        var subdirs;
        try{
          subdirs = fs.readdirSync(metadata.source + dir);
        } catch (ex) {
          console.log("Could not read dir, even though it could stat...: "+dir);
          transfer_log.insert({"log_message":"Could not read dir, even though it could stat","related_file":dir, "full_path":metadata.source + dir,"date":new Date()});
        }
        //var subdirDocs = [];
        var included_subdirs = [];
        for(var i = 0; i < subdirs.length; i++){
          if(includeFile(subdirs[i])){
            var subdir_id;
            if(strEndsWith(dir, osSep)){
              subdir_id = dir + subdirs[i];
            } else {
              subdir_id = dir + osSep + subdirs[i];
            }
            included_subdirs.push(subdir_id);
            //This db controller is synchronous, but it unfortunately can't do bulk inserts.
            directories_to_list.insert({"_id":subdir_id});
          }
        }
        // Subdirs have been inserted into DB.  
        directories_to_list.remove({"_id":dir});
        for(var i = 0; i < included_subdirs.length; i++){
          try{
            addSubdirsAndFilesRemoveDir(included_subdirs[i]);
          }catch(ex){
            console.log(ex);
            console.log("Error when addingSubDir for "+JSON.stringify(included_subdirs[i]));
            transfer_log.insert({"log_message":"Error when addingSubDir","related_file":included_subdirs[i], "date":new Date()});
          }
        }
      }
    } else {
      //could not stat file
      console.log("fs.stat came up empty...: "+JSON.stringify(metadata.source + dir));
      transfer_log.insert({"log_message":"Could not stat file","related_file":dir, "full_path":metadata.source + dir,"date":new Date()});
      directories_to_list.remove({"_id":dir});
    }
  } catch (ex) {
    console.log(ex);
    console.log("Failure for some reason "+JSON.stringify(metadata.source + dir));
    transfer_log.insert({"log_message":"Failure for some reason.","related_file":dir, "full_path":metadata.source + dir,"date":new Date()});
  }
}

function checkArgv(argv, options){
  try{
    if(fs.statSync(argv.source) && fs.statSync(argv.target)){
      return true;
    } else {
      return false;
    }
  } catch(ex) {
    console.log("Could not stat source or target: "+JSON.stringify(ex));
    return false;
  }
}

function includeFile(file){
  if( excluded_file_types != null ){
    for(var i = 0; i < excluded_file_types.length; i++){
      if(strEndsWith(file, excluded_file_types[i])){ return false; }
    }
  } 
  return true;
}

function copyFile(source, target, fileId, callback) {
  var should_copy = true;
  
  var source_stat, target_stat;
  //Check source
  try{
    source_stat = fs.statSync(source);
  } catch(ex){
    callback(ex, fileId);
  }
  
  //Check target
  try{
    fse.ensureFileSync(target);
    target_stat = fs.statSync(target);
  } catch(ex){
    callback(ex, fileId);
  }
  // If target didn't stat, or the sizes don't match
  if(target_stat === null || source_stat.size != target_stat.size){
    try{
      fse.copySync(source, target);
	  if(validate_copy){
		var source_stat_post = fs.statSync(source);
		var target_stat_post = fs.statSync(target);	  
		if(source_stat_post.size == target_stat_post.size){
		  callback(null, fileId);
		} else {
		  callback(new Error("Sizes of source and target files didn't match."), fileId);
		}
	  } else {
		//Success!  Files were copied without error, but weren't validated.
		callback(null, fileId);
	  }
    } catch ( err ){
      //Unknown failure
      callback(err, fileId);
    }
  } else {
    //Success!  Files matched before we started.
	//console.log("Files matched prior to copy");
    callback(null, fileId);
  }
}

function strEndsWith(str, suffix) {
    return str.match(suffix+"$")==suffix;
}