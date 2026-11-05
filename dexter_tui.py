"""Compact terminal layouts shared by curses and deterministic previews."""
from __future__ import annotations

from dataclasses import dataclass, field
import textwrap


@dataclass(frozen=True)
class Span:
    y: int
    x: int
    text: str
    role: str = 'body'


@dataclass
class MenuFrame:
    width: int
    height: int
    spans: list[Span] = field(default_factory=list)
    clicks: list[tuple[int,int,int,int,str]] = field(default_factory=list)
    scroll_top: int = 0

    def put(self,y,x,text,role='body'):
        if 0 <= y < self.height and 0 <= x < self.width-1:
            self.spans.append(Span(y,x,str(text)[:max(0,self.width-x-1)],role))


def compact_text(text,width):
    if len(text)<=width:
        return text
    return text[:max(0,width-3)]+'...' if width>=3 else text[:max(0,width)]


def compose_menu(width,height,*,title,options,selected=0,summary='',intro=(),
                 status='',status_role='success',filter_text='',filtering=False,detail_lines=()):
    frame=MenuFrame(width,height)
    if width<44 or height<13:
        frame.put(1,1,'DEXTER','title')
        frame.put(3,1,'Enlarge the terminal to at least 44 x 13.','warning')
        frame.put(height-2,1,'Esc / q  Back','muted')
        return frame
    frame.put(0,1,'DEXTER','title')
    frame.put(0,10,title,'heading')
    frame.put(1,1,summary,'warning' if 'LIVE SENDS' in summary else 'muted')
    frame.put(2,1,'─'*(width-3),'muted')
    top=4 if height>=20 else 3
    if filter_text or filtering:
        frame.put(top,2,'Find: '+filter_text+('|' if filtering else ''),'title')
        top+=2
    elif intro:
        frame.put(top,2,compact_text(' '.join(intro),width-5),'muted')
        top+=2 if height>=24 else 1
    wide=width>=104
    left_width=min(48,width//2-3) if wide else width-4
    detail_height=0 if wide else (3 if height>=20 else 2)
    footer=height-2
    available=max(1,footer-top-detail_height-2-(1 if status else 0))
    selected=max(0,min(selected,len(options)-1))
    start=max(0,min(selected-available+1,len(options)-available))
    frame.scroll_top=start
    for offset,option in enumerate(options[start:start+available]):
        index=start+offset
        shortcut=getattr(option,'shortcut','') or (str(index+1) if index<9 else '')
        prefix=(shortcut+'  ') if shortcut else '   '
        label=compact_text(prefix+option.label,left_width-1)
        role='selected' if index==selected else ('title' if option.value=='start' and title=='Home' else 'body')
        frame.put(top+offset,2,label.ljust(left_width),role)
        frame.clicks.append((top+offset,2,top+offset,2+left_width,option.value))
    if len(options)>available:
        frame.put(footer-detail_height-2,width-17,f'{selected+1} / {len(options)}','muted')
    option=options[selected] if options else None
    detail_x=left_width+6 if wide else 2
    detail_top=top if wide else footer-detail_height-1
    detail_width=width-detail_x-3
    if option:
        lines=list(detail_lines) if detail_lines and title=='Home' and option.value=='start' else [option.detail]
        if wide:
            frame.put(detail_top,detail_x,option.label,'heading')
            detail_top+=2
        detail_limit=footer-detail_top-2 if wide else detail_height
        row=detail_top
        for line in lines:
            for wrapped in textwrap.wrap(line,width=max(8,detail_width)) or ['']:
                if row>=detail_top+detail_limit:
                    break
                frame.put(row,detail_x,wrapped,'body' if wide else 'muted')
                row+=1
            if wide:
                row+=1
    if status:
        frame.put(height-3,2,status,status_role)
    controls=('Type to filter · Enter select · Esc clear' if filtering else
              '↑↓ move  Enter open  / find  Esc/q '+('quit' if title=='Home' else 'back'))
    frame.put(footer,2,controls,'muted')
    return frame


def compose_message(width,height,*,title,lines,summary='',scroll_top=0,button_label='Back'):
    """A readable, scrollable body for help, history and diagnostic messages."""
    frame=MenuFrame(width,height)
    if width<44 or height<13:
        frame.put(1,1,'DEXTER','title')
        frame.put(3,1,'Enlarge the terminal to at least 44 x 13.','warning')
        frame.put(height-2,1,'Enter / Esc  Back','muted')
        return frame
    frame.put(0,1,'DEXTER','title')
    frame.put(0,10,title,'heading')
    frame.put(1,1,summary,'warning' if 'LIVE SENDS' in summary else 'muted')
    frame.put(2,1,'─'*(width-3),'muted')
    rows=[]
    for line in lines:
        for paragraph in str(line).split('\n'):
            rows.extend(textwrap.wrap(paragraph,width=width-4) or [''])
    body_top=4 if height>=20 else 3
    button_y=height-4
    visible=max(1,button_y-body_top-1)
    start=max(0,min(scroll_top,max(0,len(rows)-visible)))
    frame.scroll_top=start
    for index,line in enumerate(rows[start:start+visible]):
        frame.put(body_top+index,2,line)
    if len(rows)>visible:
        frame.put(button_y-1,2,f'Lines {start+1}-{min(start+visible,len(rows))} of {len(rows)}','muted')
    label='  '+button_label+'  '
    frame.put(button_y,2,label,'selected')
    frame.clicks.append((button_y,2,button_y,2+len(label),'back'))
    frame.put(height-2,2,'↑↓ scroll  PgUp/PgDn  Enter/Esc back','muted')
    return frame
