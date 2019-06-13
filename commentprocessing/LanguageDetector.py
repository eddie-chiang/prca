import cld2


class LanguageDetector:
    """A language detector class determines whether a given comment is likely to be in the specified language."""

    @staticmethod
    def is_english(comment):
        """Determines if the given comment is in English.

        Args:
            comment (str): A text.

        Returns:
            bool: True if is English, False otherwise.
        """
        is_reliable, _, details = cld2.detect(comment)

        i = 0
        for detail in details:
            if i == 0 and is_reliable:
                # Top language is much better than the 2nd best language, so just rely on the first result.
                return True if detail.language_name == 'ENGLISH' else False
            elif detail.language_name == 'ENGLISH':
                # English being one of the top 3 probable language.
                return True
            i += 1

        return False
