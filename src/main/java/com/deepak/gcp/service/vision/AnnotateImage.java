package com.deepak.gcp.service.vision;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.cloud.vision.v1.AnnotateImageRequest;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.BatchAnnotateImagesResponse;
import com.google.cloud.vision.v1.EntityAnnotation;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.Feature.Type;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * A snippet for Google Cloud Vision API demonstrating how to determine what is shown on a picture.
 */
public class AnnotateImage {
  public static void main(String... args) throws Exception {
    // Instantiates a client
    ImageAnnotatorClient vision = ImageAnnotatorClient.create();

    // The path to the image file to annotate
    String fileName = "your/image/path.jpg"; // for example "./resources/wakeupcat.jpg";

    // Reads the image file into memory
    Path path = Paths.get(fileName);
    byte[] data = Files.readAllBytes(path);
    ByteString imgBytes = ByteString.copyFrom(data);

    // Builds the image annotation request
    List<AnnotateImageRequest> requests = new ArrayList<>();
    Image img = Image.newBuilder().setContent(imgBytes).build();
    Feature feat = Feature.newBuilder().setType(Type.LABEL_DETECTION).build();
    AnnotateImageRequest request =
        AnnotateImageRequest.newBuilder().addFeatures(feat).setImage(img).build();
    requests.add(request);

    // Performs label detection on the image file
    BatchAnnotateImagesResponse response = vision.batchAnnotateImages(requests);
    List<AnnotateImageResponse> responses = response.getResponsesList();

    for (AnnotateImageResponse res : responses) {
      if (res.hasError()) {
        System.out.printf("Error: %s\n", res.getError().getMessage());
        return;
      }

      for (EntityAnnotation annotation : res.getLabelAnnotationsList()) {
        for (Map.Entry<FieldDescriptor, Object> entry : annotation.getAllFields().entrySet()) {
          System.out.printf("%s : %s\n", entry.getKey(), entry.getValue());
        }
      }
    }
  }
}
