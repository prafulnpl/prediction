import clip
import torch
from PIL import Image
from utils.labels import labels


# Load CLIP model
device = "cuda" if torch.cuda.is_available() else "cpu"
model, preprocess = clip.load("ViT-B/32", device)


# Load and preprocess the image
img_path = "/Users/prafulnepal/Downloads/pepecoin.jpeg"  
image = preprocess(Image.open(img_path)).unsqueeze(0).to(device)

# Tokenize the labels dynamically (your custom list of labels)
text_inputs = torch.cat([clip.tokenize(label) for label in labels]).to(device)

# Compute features for both image and text using CLIP
with torch.no_grad():
    image_features = model.encode_image(image)
    text_features = model.encode_text(text_inputs)

# Normalize the features
image_features /= image_features.norm(dim=-1, keepdim=True)
text_features /= text_features.norm(dim=-1, keepdim=True)

# Compute similarity between the image and text labels
similarity = (image_features @ text_features.T)

# Get the top-5 predicted labels with the highest similarity scores
top5_values, top5_indices = similarity[0].topk(5)

# Display the top-5 predicted labels and their similarity scores
print("Top-5 Predicted Labels:")
for i in range(top5_values.size(0)):
    label = labels[top5_indices[i]]
    score = top5_values[i].item()
    print(f"{label}: {score:.4f}")


