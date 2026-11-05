from __future__ import annotations

import curses
from types import SimpleNamespace
from unittest import TestCase, mock

from dexter_tui import compose_menu, compose_message
from Dexter import InteractiveDexterCLI, TuiOption, TUI_SETTINGS_FIELDS


OPTIONS=[TuiOption('Start Dexter','start','Use saved settings.','s'),
         TuiOption('Positions','positions','Inspect recovery positions.','p'),
         TuiOption('Settings','settings','Configure the application.','e')]


class TerminalNavigationTests(TestCase):
    def test_every_size_keeps_selected_setting_visible_and_all_cells_in_bounds(self):
        options=[TuiOption('Setting '+str(i),str(i),'Description') for i in range(40)]
        for width,height in [(112,32),(80,24),(60,20),(44,13)]:
            with self.subTest(size=(width,height)):
                frame=compose_menu(width,height,title='Settings',options=options,selected=39,
                    summary='Mainnet  Simulate · no sends',intro=['Select a setting.'])
                self.assertTrue(any(span.role=='selected' and 'Setting 39' in span.text for span in frame.spans))
                self.assertTrue(any('Enter open' in span.text for span in frame.spans))
                self.assertTrue(all(0<=span.y<height and 0<=span.x and span.x+len(span.text)<width for span in frame.spans))

    def app(self, keys):
        app=InteractiveDexterCLI.__new__(InteractiveDexterCLI)
        app.screen=mock.Mock()
        app.screen.getmaxyx.return_value=(24,80)
        app.screen.get_wch.side_effect=keys
        app.status_message=''
        app._header_summary=mock.Mock(return_value='Mainnet Simulate · no sends')
        app._attr=mock.Mock(return_value=0)
        app._write=mock.Mock()
        return app

    def test_home_hotkey_opens_a_task(self):
        self.assertEqual(self.app(['p'])._choose('Home',OPTIONS),'positions')

    def test_filter_enter_selects_matching_setting(self):
        keys=['/','c','o','n','f','\n']
        self.assertEqual(self.app(keys)._choose('Home',OPTIONS),'settings')

    def test_escape_clears_filter_before_leaving_menu(self):
        keys=['/','z','\x1b','p']
        self.assertEqual(self.app(keys)._choose('Home',OPTIONS),'positions')

    def test_resize_keeps_navigation_operational(self):
        self.assertEqual(self.app([curses.KEY_RESIZE,curses.KEY_END,'\n'])._choose('Home',OPTIONS),'settings')

    def test_filtered_home_result_shows_its_own_detail(self):
        for option in OPTIONS[1:]+[TuiOption('No matching items','__empty','Clear the search.')]:
            frame=compose_menu(80,24,title='Home',options=[option],detail_lines=['Start-only description'])
            self.assertTrue(any(option.detail in span.text for span in frame.spans))
            self.assertFalse(any('Start-only' in span.text for span in frame.spans))

    def test_clicking_empty_search_result_keeps_search_open(self):
        app=self.app(['/','z',curses.KEY_MOUSE,'\x1b','p'])
        app._mouse_activates_selection=mock.Mock(return_value=True)
        with mock.patch('curses.getmouse',return_value=(0,3,6,0,1)):
            self.assertEqual(app._choose('Home',OPTIONS),'positions')

    def test_message_body_is_complete_and_scrolls_to_its_final_line(self):
        lines=['Line '+str(i) for i in range(60)]+['Final recovery instruction']
        for width,height in [(112,32),(80,24),(60,20),(44,13)]:
            with self.subTest(size=(width,height)):
                first=compose_message(width,height,title='Help',lines=lines)
                last=compose_message(width,height,title='Help',lines=lines,scroll_top=1_000_000)
                self.assertTrue(any(span.text=='Line 0' for span in first.spans))
                self.assertTrue(any(span.text=='Final recovery instruction' for span in last.spans))
                self.assertGreater(last.scroll_top,0)
                self.assertTrue(all(0<=span.y<height and span.x+len(span.text)<width for span in last.spans))

    def test_message_end_key_reveals_last_instruction_and_enter_returns(self):
        app=self.app([curses.KEY_END,'\n'])
        app._safe_config=mock.Mock(return_value=(None,None))
        app._message('Help',['Line '+str(i) for i in range(60)]+['Final recovery instruction'])
        self.assertTrue(any(call.args[2]=='Final recovery instruction' for call in app._write.call_args_list))

    def test_sensitive_config_is_not_rendered_in_rows(self):
        app=self.app([])
        app.example_env={}
        rendered=app._display_env_value(TUI_SETTINGS_FIELDS['PRIVATE_KEY'],{'PRIVATE_KEY':'full-private-key'})
        self.assertEqual(rendered,'configured (hidden)')
        rendered=app._display_env_value(TUI_SETTINGS_FIELDS['HTTP_URLS'],
            {'HTTP_URLS':'https://rpc.example/path-secret?api-key=private-rpc-key'})
        self.assertEqual(rendered,'rpc.example')
