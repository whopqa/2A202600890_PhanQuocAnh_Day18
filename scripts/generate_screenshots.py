import os
import json
from PIL import Image, ImageDraw, ImageFont

def draw_text_image(output_path, title, lines, font_path="C:\\Windows\\Fonts\\consola.ttf", font_size=16):
    # Calculate image size
    # We want padding and standard dimensions
    line_height = font_size + 4
    margin = 30
    width = 900
    height = len(lines) * line_height + 2 * margin + 40
    
    # Create background image
    # A modern dark theme (almost black/deep blue gray)
    bg_color = (30, 30, 30)
    text_color = (220, 220, 220)
    accent_color = (0, 162, 232) # Cyan
    
    image = Image.new("RGB", (width, height), bg_color)
    draw = ImageDraw.Draw(image)
    
    try:
        font = ImageFont.truetype(font_path, font_size)
        title_font = ImageFont.truetype(font_path, font_size + 4)
    except IOError:
        font = ImageFont.load_default()
        title_font = ImageFont.load_default()
        
    # Draw title
    draw.text((margin, margin), title, fill=accent_color, font=title_font)
    draw.line((margin, margin + font_size + 15, width - margin, margin + font_size + 15), fill=(80, 80, 80), width=1)
    
    # Draw lines
    y = margin + font_size + 30
    for line in lines:
        draw.text((margin, y), line, fill=text_color, font=font)
        y += line_height
        
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    image.save(output_path)
    print(f"Generated screenshot: {output_path}")

def build_tree_lines(dir_path, prefix=""):
    lines = []
    if not os.path.exists(dir_path):
        return [f"Directory {dir_path} does not exist"]
    
    entries = sorted(os.listdir(dir_path))
    # Filter out parquet part files if they clutter the tree, but show _delta_log and folder structure
    entries = [e for e in entries if not e.endswith(".parquet") or e == entries[0] or len(entries) <= 3]
    
    # Let the user know if we're truncating large files lists
    has_more_parquet = len(os.listdir(dir_path)) > len(entries)
    
    for i, entry in enumerate(entries):
        path = os.path.join(dir_path, entry)
        is_last = (i == len(entries) - 1) and not has_more_parquet
        connector = "└── " if is_last else "├── "
        
        lines.append(f"{prefix}{connector}{entry}")
        
        if os.path.isdir(path):
            new_prefix = prefix + ("    " if is_last else "│   ")
            lines.extend(build_tree_lines(path, new_prefix))
            
    if has_more_parquet:
        lines.append(f"{prefix}└── ... (other parquet files hidden for readability)")
        
    return lines

def main():
    # 1. Directory Tree screenshot
    lakehouse_dir = "_lakehouse"
    tree_lines = [f"{lakehouse_dir}/"] + build_tree_lines(lakehouse_dir)
    draw_text_image(
        output_path="submission/screenshots/lakehouse_tree.png",
        title="Terminal: tree _lakehouse/",
        lines=tree_lines,
        font_size=15
    )
    
    # 2. Delta Log JSON screenshot
    log_file = "_lakehouse/bronze/llm_calls_raw/_delta_log/00000000000000000000.json"
    if os.path.exists(log_file):
        json_lines = []
        with open(log_file, "r") as f:
            for line in f:
                parsed = json.loads(line)
                formatted = json.dumps(parsed, indent=2)
                json_lines.extend(formatted.splitlines())
        
        # Limit to first 40 lines for a readable screenshot
        max_lines = 40
        if len(json_lines) > max_lines:
            json_lines = json_lines[:max_lines] + ["...", "// truncated for display"]
            
        draw_text_image(
            output_path="submission/screenshots/delta_log_json.png",
            title=f"File: {log_file} (Delta Transaction Log v0)",
            lines=json_lines,
            font_size=14
        )
    else:
        print(f"Log file not found: {log_file}")

if __name__ == "__main__":
    main()
