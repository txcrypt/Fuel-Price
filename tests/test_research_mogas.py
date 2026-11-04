from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'research'))
from watc_mogas import parse_quote


SAMPLE='''Daily Report 2 January 2024
Commodities US$
Mogas95* 91.39 $0.04
*Mogas95 is the Singapore benchmark petrol price.
Changes since the previous Daily on 15 December 2023
'''


class MogasExtractionTests(unittest.TestCase):
    def test_holiday_change_is_retained_without_fabricating_daily_return(self):
        result=parse_quote(SAMPLE,'020124_daily-report.pdf')
        self.assertEqual(result['mogas95_quote'],91.39)
        self.assertEqual(result['displayed_change'],.04)
        self.assertIn('15 December 2023',result['change_notes'])
        self.assertEqual(result['quote_observed_date'],'')
        self.assertFalse(result['verified_point_in_time_ready'])

    def test_filename_date_mismatch_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'dates disagree'):
            parse_quote(SAMPLE,'03012024_daily-report.pdf')

    def test_missing_quote_is_not_imputed(self):
        with self.assertRaisesRegex(ValueError,'numeric Mogas95 row'):
            parse_quote(SAMPLE.replace('Mogas95* 91.39 $0.04',''),'020124_daily-report.pdf')

    def test_multiple_numeric_quotes_require_review(self):
        with self.assertRaisesRegex(ValueError,'exactly one'):
            parse_quote(SAMPLE+'Mogas95* 92.00 $0.61','020124_daily-report.pdf')

    def test_currency_and_benchmark_identity_are_required(self):
        for bad in [SAMPLE.replace('US$','AUD$'),SAMPLE.replace('Singapore benchmark','another benchmark')]:
            with self.assertRaises(ValueError):parse_quote(bad,'020124_daily-report.pdf')


if __name__=='__main__':unittest.main()
