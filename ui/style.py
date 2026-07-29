# style.py - Soft Pastel CLI Theme & Layout System for AELVO OMEGA

import sys

# Soft pastel theme palette (Low light, muted tones, professional appearance)
C_PRIMARY = "\033[38;5;146m"    # Soft pastel lilac
C_ACCENT = "\033[38;5;151m"     # Muted sky blue
C_SUCCESS = "\033[38;5;151m"   # Soft mint green
C_WARNING = "\033[38;5;223m"   # Soft peach
C_DANGER = "\033[38;5;174m"    # Muted rose
C_MUTED = "\033[38;5;245m"     # Soft gray
C_WHITE = "\033[38;5;252m"     # Off-white
C_RESET = "\033[0m"            # Standard reset

# Formatting styles
BOLD = "\033[1m"
DIM = "\033[2m"
ITALIC = "\033[3m"
UNDERLINE = "\033[4m"

# Soft status symbols
SYM_OK = f"{C_SUCCESS}✓{C_RESET}"
SYM_INFO = f"{C_PRIMARY}ℹ{C_RESET}"
SYM_WARN = f"{C_WARNING}⚠{C_RESET}"
SYM_FAIL = f"{C_DANGER}✗{C_RESET}"
SYM_BULLET = f"{C_ACCENT}·{C_RESET}"

def print_styled(text: str, color: str = C_RESET, bold: bool = False, dim: bool = False):
    """Outputs text with styled typography directly to stdout."""
    prefix = ""
    if bold: prefix += BOLD
    if dim: prefix += DIM
    sys.stdout.write(f"{prefix}{color}{text}{C_RESET}\n")

def draw_header(title: str, subtitle: str = ""):
    """Draws a clean minimal header."""
    width = 70
    separator = "─" * width
    print_styled(separator, C_MUTED, dim=True)
    
    # Title centered
    title_padded = f" {title} ".center(width, " ")
    print_styled(title_padded, C_WHITE, bold=True)
    
    if subtitle:
        sub_padded = f" {subtitle} ".center(width, " ")
        print_styled(sub_padded, C_MUTED)
        
    print_styled(separator, C_MUTED, dim=True)

def draw_box_row(content: str, color: str = C_WHITE, bold: bool = False):
    """Draws a single formatted item row."""
    width = 70
    content_padded = content.ljust(width - 2)
    print_styled(f" {content_padded}", color, bold=bold)

def draw_separator():
    """Draws a clean minimal horizontal separating line."""
    print_styled("─" * 70, C_MUTED, dim=True)
