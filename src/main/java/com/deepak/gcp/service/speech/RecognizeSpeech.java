
package com.deepak.gcp.service.speech;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.google.cloud.speech.v1.RecognitionAudio;
import com.google.cloud.speech.v1.RecognitionConfig;
import com.google.cloud.speech.v1.RecognitionConfig.AudioEncoding;
import com.google.cloud.speech.v1.RecognizeResponse;
import com.google.cloud.speech.v1.SpeechClient;
import com.google.cloud.speech.v1.SpeechRecognitionAlternative;
import com.google.cloud.speech.v1.SpeechRecognitionResult;
import com.google.protobuf.ByteString;

/**
 * A snippet for Google Natural Language API showing how to convert human speech from an audio file
 * into a text form.
 */
public class RecognizeSpeech {
  public static void main(String... args) throws Exception {
    SpeechClient speech = SpeechClient.create();

    // The path to the audio file to transcribe
    String fileName = "your/speech/audio/file.raw"; // for example "./resources/audio.raw";

    // Reads the audio file into memory
    Path path = Paths.get(fileName);
    byte[] data = Files.readAllBytes(path);
    ByteString audioBytes = ByteString.copyFrom(data);

    // Builds the sync recognize request
    RecognitionConfig config =
        RecognitionConfig.newBuilder()
            .setEncoding(AudioEncoding.LINEAR16)
            .setSampleRateHertz(16000)
            .setLanguageCode("en-US")
            .build();
    RecognitionAudio audio = RecognitionAudio.newBuilder().setContent(audioBytes).build();

    // Performs speech recognition on the audio file
    RecognizeResponse response = speech.recognize(config, audio);
    List<SpeechRecognitionResult> results = response.getResultsList();

    for (SpeechRecognitionResult result : results) {
      List<SpeechRecognitionAlternative> alternatives = result.getAlternativesList();
      for (SpeechRecognitionAlternative alternative : alternatives) {
        System.out.printf("Transcription: %s%n", alternative.getTranscript());
      }
    }
    speech.close();
  }
}
