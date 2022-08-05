import datetime
import subprocess
from google.cloud import storage
from google.cloud import speech_v1 as speech
from decouple import config
from pydub import AudioSegment
import timeit
import multiprocessing
from my_queue import MyQueue
import logging
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config('KEY')

directory = config('AUDIO_DIRECTORY')

storage_client = storage.Client()
bucket = storage_client.get_bucket(config('BUCKET'))
wav_folder=config('WAV_FOLDER')
audio_folder_name = config('AUDIO_FOLDER_NAME')

client = speech.SpeechClient()
base_gcs_address = "gs://"+str(bucket.name)+"/"+audio_folder_name+"/"

result_directory = config('RESULT_DIRECTORY')

error_audio_directory=config('ERROR_AUDIO_DIRECTORY')

logging.basicConfig(filename = 'file.log',level = logging.DEBUG,format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')

def convert(directory,filename):
    path=os.path.join(directory,filename)
    pa = datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f') + '.wav'
    to_path = os.path.join(wav_folder, pa)
    cmd = 'ffmpeg -i "{0}" -acodec pcm_s16le -ar 16000 "{1}" -y'.format(path, to_path)
    p = subprocess.Popen(cmd, shell=True, stdout=(subprocess.PIPE), stderr=(subprocess.STDOUT))
    p.wait()
    sound = AudioSegment.from_wav(to_path)
    sound = sound.set_channels(1)
    sound.export(to_path, format="wav")
    pa=os.path.basename(to_path)
    return pa


def upload_blob(bucket_name, source_file_name, destination):
    blob = bucket_name.blob(destination+"/"+source_file_name)
    blob.upload_from_filename(os.path.join(wav_folder, source_file_name))
    
def list_blobs_with_prefix(bucket_name, prefix, delimiter):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)
    for blob in blobs:
        if '.' in blob.name:
            q.put(os.path.basename(blob.name))

def speech_to_text(filename):
    try:
        start = timeit.default_timer()
        time_out = 20000
        gcs_uri = base_gcs_address+filename

        audio = speech.RecognitionAudio(uri=gcs_uri)
            
        config = speech.RecognitionConfig(
            encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
            enable_automatic_punctuation=True,
            language_code="ja-JP",
            )
        
        operation = client.long_running_recognize(config=config, audio=audio)

        print("Waiting for operation to complete...", filename)
        logging.info('Waiting for operation to complete...'+filename)
        response = operation.result(timeout=time_out)
        result = response.results[-1]

        f = open(result_directory+filename+".txt", "a", encoding="utf-8")
            
        for result in response.results:
            f.write("{}".format(result.alternatives[0].transcript))
            f.write("\n")
        f.close()

        print(filename+" file is completed")
        logging.info(filename+' file is completed')

        stop = timeit.default_timer()
        pid = os.getpid()
        #print("PID : "+str(pid))
        logging.debug("PID : "+str(pid))

        long_time = stop - start
        f = open(result_directory+"time.txt", "a", encoding="utf-8")
        f.write(filename+" : "+str(long_time))
        f.write("\n")
        f.close()
        #print("Time : "+str(long_time))
        logging.debug("Time : "+str(long_time)+"seconds")

        path=os.path.join(wav_folder,filename)
        size = os.path.getsize(path)
        size = size*0.001
        #print("File Size : "+str(size)+" KB")
        logging.debug("File Size : "+str(size)+" KB")


    except:
        print("Failed to complete...", filename)
        logging.info('Failed to complete...'+filename)
        path=os.path.join(wav_folder,filename)
        new_path=os.path.join(error_audio_directory,filename)
        os.rename(path, new_path)

def delete_blob(bucket_name):
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=audio_folder_name)
    for blob in blobs:
        if '.' in blob.name:
            blob.delete()
            logging.debug('Blob is deleted.')


def check_queue_size(qsize):
    processes = []

    while not q.empty():
        if(q.qsize()>4):
            for _ in range(4):
                p = multiprocessing.Process(target=speech_to_text,args=[q.get()])
                p.start()
                processes.append(p)
            
        else:
            k=q.qsize()
            for _ in range(k):
                p = multiprocessing.Process(target=speech_to_text,args=[q.get()])
                p.start()
                processes.append(p)

        for process in processes:
            process.join()
    
    logging.info("Process Finished.")
    #print("==========================END============================")
    logging.debug('==========================END============================')

def delete_audio_files(filename):
    path=os.path.join(directory,filename)
    os.remove(path)


def delete_wav_audio_files(filename):
    path=os.path.join(wav_folder,filename)
    os.remove(path)

if __name__ == "__main__": 
    q=MyQueue()

    delete_blob(bucket)

    for filename in os.listdir(directory):
        if filename.endswith(".mp3") or filename.endswith(".wav"):
            try:
                pa=convert(directory,filename)
                logging.debug(filename+' audio file has been successfully converted to WAV format.')
                try:
                    upload_blob(bucket,pa,audio_folder_name)
                    logging.debug(pa+' audio file has been uploaded to Bucket.')
                except:
                    logging.error(pa+' audio file has been failed to upload to Bucket.')
            except:
                logging.error(filename+' audio file has been failed to convert to WAV format.')       
        else:
            logging.warning('Only .mp3 and .wav file types are allowed!')
        
    for filename in os.listdir(directory):
        try:
            delete_audio_files(filename)
            logging.debug(filename+' audio file has been deleted from audio folder.')
        except:
            logging.error(filename+' audio file has been failed to delete from audio folder.')

    list_blobs_with_prefix(bucket,audio_folder_name+'/','/')

    logging.debug("==========================START============================")

    check_queue_size(q.qsize())

    for pa in os.listdir(wav_folder): 
        try:
            delete_wav_audio_files(pa)
            logging.debug(pa+' audio file has been deleted from wav folder.')
        except:
            logging.error(pa+' audio file has been failed to delete from wav folder.')

    delete_blob(bucket)

   
