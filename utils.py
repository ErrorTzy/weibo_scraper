def extracted_text_from_html(text_html):
    """
    DESCRIPTION:
        a fast version of html.text with pure regex

    INPUT: 
        html data in string
    
    OUTPUT: 
        html data without tags and &xx; charactors
    """
    try:
        re
    except NameError:
        import re
    return re.sub(r'(&[a-z]*?;)|(<[^>]*>)', '', text_html)

