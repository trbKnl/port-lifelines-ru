import logging
import json
import io

import pandas as pd

from port.api.commands import (CommandSystemDonate, CommandSystemExit, CommandUIRender)
import port.api.props as props
import port.facebook as facebook


LOG_STREAM = io.StringIO()

logging.basicConfig(
    stream=LOG_STREAM,
    level=logging.INFO,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")

# Headers
SUBMIT_FILE_HEADER = props.Translatable({
    "en": "Select your Facebook file", 
    "nl": "Selecteer uw Facebook bestand"
})

REVIEW_DATA_HEADER = props.Translatable({
    "en": "Your Facebook data", 
    "nl": "Uw Facebook gegevens"
})

RETRY_HEADER = props.Translatable({
    "en": "Try again", 
    "nl": "Probeer opnieuw"
})


def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platform = ("Facebook", extract_facebook, facebook.validate)
    platform_name, extraction_fun, validation_fun = platform

    table_list = None
    group_list = []
    selected_groups = []

    # Prompt file extraction loop
    while True:
        LOGGER.info("Prompt for file for %s", platform_name)
        yield donate_logs(f"{session_id}-tracking")

        # Render the propmt file page
        file_prompt = generate_file_prompt("application/zip")
        file_result = yield render_page(platform_name, file_prompt)

        if file_result.__type__ == "PayloadString":
            validation = validation_fun(file_result.value)

            # DDP is recognized: Status code zero
            if validation.status_code.id == 0: 
                LOGGER.info("Payload for %s", platform_name)
                yield donate_logs(f"{session_id}-tracking")

                table_list = extraction_fun(file_result.value, validation)
                group_list = facebook.groups_to_list(file_result.value)
                break

            # DDP is not recognized: Different status code
            if validation.status_code.id != 0: 
                LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                yield donate_logs(f"{session_id}-tracking")
                retry_result = yield render_page(platform_name, retry_confirmation(platform_name))

                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    LOGGER.info("Skipped during retry %s", platform_name)
                    yield donate_logs(f"{session_id}-tracking")
                    break
        else:
            LOGGER.info("Skipped %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")
            break

    # Render data on screen
    if table_list is not None:
        LOGGER.info("Prompt consent; %s", platform_name)
        yield donate_logs(f"{session_id}-tracking")

        # Check if extract something got extracted
        if len(table_list) == 0:
            table_list.append(create_empty_table(platform_name))

        consent_form_prompt = create_consent_form(table_list)
        consent_result = yield render_page(platform_name, consent_form_prompt)

        if consent_result.__type__ == "PayloadJSON":
            LOGGER.info("Data donated; %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")
            yield donate(platform_name, consent_result.value)

            # If donation render checkbox list
            if len(group_list) > 0:
                render_questionnaire_results = yield render_checkbox_question(group_list)
                if render_questionnaire_results.__type__ == "PayloadJSON":
                    yield donate(f"{session_id}-checkbox-donation", render_questionnaire_results.value)
                    selected_groups = parse_questionnaire_json(render_questionnaire_results.value)

                else:
                    LOGGER.info("Skipped questionnaire: %s", platform_name)
                    yield donate_logs(f"tracking-{session_id}")

            if len(selected_groups) > 0:
                render_questionnaire_results = yield render_multiple_choice_questions(selected_groups)
                if render_questionnaire_results.__type__ == "PayloadJSON":
                    yield donate(f"{session_id}-multiple-choice", render_questionnaire_results.value)
                else:
                    LOGGER.info("Skipped questionnaire: %s", platform_name)
                    yield donate_logs(f"tracking-{session_id}")

        else:
            LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
            yield donate_logs(f"{session_id}-tracking")

    yield exit(0, "Success")
    yield render_end_page()


##################################################################

def parse_questionnaire_json(json_str: str) -> list["str"]:
    out = []
    try:
        out = json.loads(json_str).get("1", [])
    except Exception as e:
        LOGGER.error(e)

    return out



def create_consent_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, meta_tables=[])


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def donate_status(filename: str, message: str):
    return donate(filename, json.dumps({"status": message}))



##################################################################
# Extraction function

def extract_facebook(facebook_zip: str, _) -> list[props.PropsUIPromptConsentFormTable]:
    tables_to_render = []

    username = facebook.get_username(facebook_zip)
    replacement = "<Jouw gebruikersnaam>"

    email_replacement = "<Jouw emailadres>"
    phone_replacement = "<Jouw telefoonnummer>"

    emails = facebook.get_emails(facebook_zip)
    numbers = facebook.get_phone_numbers(facebook_zip)

    df = facebook.who_youve_followed_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)

    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "who_youve_followed"
        table_title = props.Translatable({
            "en": "Who you've followed", 
            "nl": "Who you've followed", 
        })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.your_friends_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "your_friends"
        table_title = props.Translatable(
            {
                "en": "Your friends", 
                "nl": "Your friends", 
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.ads_interests_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "ads_interests"
        table_title = props.Translatable(
            {
                "en": "Ads interests", 
                "nl": "Ads interests", 
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.recently_visited_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "recently_visited"
        table_title = props.Translatable(
            {
                "en": "Recently visited", 
                "nl": "Recently visited", 
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.profile_information_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "profile_information"
        table_title = props.Translatable(
            {
                "en": "Profile information", 
                "nl": "Profile information", 
             }
        )
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.your_event_responses_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "your_event_responses"
        table_title = props.Translatable(
            {
                "en": "Your event responses", 
                "nl": "Your event responses", 
             }
        )
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.group_posts_and_comments_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "group_posts_and_comments"
        table_title = props.Translatable(
            {
                "en": "Group posts and comments", 
                "nl": "Group posts and comments", 
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.your_comments_in_groups_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "your_comments_in_groups"
        table_title = props.Translatable(
            {
                "en": "Your comments in groups",
                "nl": "Your comments in groups",
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.your_group_membership_activity_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "your_group_membership_activity_to_df"
        table_title = props.Translatable(
            {
                "en": "Your group membership activity",
                "nl": "Your group membership activity",
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)


    df = facebook.pages_youve_liked_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "pages_youve_liked"
        table_title = props.Translatable(
            {
                "en": "Pages you've liked",
                "nl": "Pages you've liked",
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)

    df = facebook.comments_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "comments"
        table_title = props.Translatable(
            {
                "en": "Your comments",
                "nl": "Your comments",
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)

    df = facebook.likes_and_reactions_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "likes_and_reactions"
        table_title = props.Translatable(
            {
                "en": "Your likes and reactions",
                "nl": "Your likes and reactions",
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)

    df = facebook.your_comment_active_days_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "your_comment_active_days"
        table_title = props.Translatable(
            {
                "en": "Your comment active days",
                "nl": "Your comment active days",
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)

    df = facebook.your_pages_to_df(facebook_zip)
    df = facebook.replace_in_df(df, username, replacement)
    for n in numbers:
        df = facebook.replace_in_df(df, n, phone_replacement)
    for e in emails:
        df = facebook.replace_in_df(df, e, email_replacement)

    if not df.empty:
        table_id = "your_pages"
        table_title = props.Translatable(
            {
                "en": "Your pages",
                "nl": "Your pages",
             })
        table =  props.PropsUIPromptConsentFormTable(table_id, table_title, df)
        tables_to_render.append(table)

    return tables_to_render



def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_page(header_text, body):
    platform = "Facebook"
    header = props.PropsUIHeader(props.Translatable({"en": header_text, "nl": header_text}))
    footer = props.PropsUIFooter()
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)



def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def generate_file_prompt(extensions):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device.",
            "nl": f"Volg de download instructies en kies het bestand dat u opgeslagen heeft op uw apparaat."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


def exit(code, info):
    return CommandSystemExit(code, info)


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Er ging niks mis, maar we konden niks vinden",
       "nl": "Er ging niks mis, maar we konden niks vinden"
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table
 

############################################

GROUP_QUESTION = props.Translatable(
    {
        "nl": "Welke Facebook-groepen vind je het belangrijkst? Selecteer maximaal drie:", 
        "en": "Welke Facebook-groepen vind je het belangrijkst? Selecteer maximaal drie:", 
     })


def render_checkbox_question(group_list: list):

    choices = [props.Translatable({"en": f"{item}", "nl": f"{item}"}) for item in group_list]
    questions = [
        props.PropsUIQuestionMultipleChoiceCheckbox(question=GROUP_QUESTION, id=1, choices=choices),
    ]

    description = props.Translatable(
        {
            "nl": "We willen weten welke Facebook-groepen voor jou belangrijk zijn. Selecteer maximaal drie groepen die voor jou het meest betekenen. 'Belangrijk' betekent dat deze groepen je emotionele steun, een gevoel van saamhorigheid, waardevolle informatie of andere hulp bieden.",
            "en": "We willen weten welke Facebook-groepen voor jou belangrijk zijn. Selecteer maximaal drie groepen die voor jou het meest betekenen. 'Belangrijk' betekent dat deze groepen je emotionele steun, een gevoel van saamhorigheid, waardevolle informatie of andere hulp bieden.",
         })
    header = props.PropsUIHeader(props.Translatable(
        {
            "nl": "Belangrijke Facebook Groepen",
            "en": "Belangrijke Facebook Groepen"
         })
    )
    body = props.PropsUIPromptQuestionnaire(questions=questions, description=description)
    footer = props.PropsUIFooter()

    page = props.PropsUIPageDonation("groups", header, body, footer)
    return CommandUIRender(page)



def render_multiple_choice_questions(group_names: list[str]):

    choices = [
        props.Translatable(
            {
                "en": "1. Helemaal mee oneens", 
                "nl": "1. Helemaal mee oneens", 
            }
        ),
        props.Translatable(
            {
                "en": "2. Mee oneens", 
                "nl": "2. Mee oneens", 
            }
        ),
        props.Translatable(
            {
                "en": "3. Neutraal", 
                "nl": "3. Neutraal", 
            }
        ),
        props.Translatable(
            {
                "en": "4. Mee eens", 
                "nl": "4. Mee eens", 
            }
        ),
        props.Translatable(
            {
                "en": "5. Helemaal mee eens", 
                "nl": "5. Helemaal mee eens", 
            }
        ),
    ] 

    questions = []
    for i, group_name in enumerate(group_names):
        question = props.Translatable(
            {
                "en": f"Ik voel me echt deel van de groep ({group_name})",
                "nl": f"Ik voel me echt deel van de groep ({group_name})",
            }
        )
        questions.append(
            props.PropsUIQuestionMultipleChoice(question=question, id=f"{i}_part_group_{group_name}", choices=choices),
        )
        question = props.Translatable(
            {
                "en": f"De groep ({group_name}) biedt mij de ondersteuning die ik nodig heb.",
                "nl": f"De groep ({group_name}) biedt mij de ondersteuning die ik nodig heb.",
            }
        )
        questions.append(
            props.PropsUIQuestionMultipleChoice(question=question, id=f"{i}_support_{group_name}", choices=choices),
        )

    description = props.Translatable(
        {
            "nl": "In de volgende sectie vragen we je om aan te geven hoezeer je het eens bent met een aantal stellingen over de groepen die je hebt geselecteerd. Dit helpt ons te begrijpen hoe deze groepen je ondersteunen en in hoeverre je je ermee verbonden voelt. Gebruik de onderstaande schaal om je antwoorden te geven:",
            "en": "In de volgende sectie vragen we je om aan te geven hoezeer je het eens bent met een aantal stellingen over de groepen die je hebt geselecteerd. Dit helpt ons te begrijpen hoe deze groepen je ondersteunen en in hoeverre je je ermee verbonden voelt. Gebruik de onderstaande schaal om je antwoorden te geven:",
         })
    header = props.PropsUIHeader(props.Translatable({"en": "Belangrijke Facebook Groepen", "nl": "Belangrijke Facebook Groepen"}))
    body = props.PropsUIPromptQuestionnaire(questions=questions, description=description)
    footer = props.PropsUIFooter()

    page = props.PropsUIPageDonation("groups", header, body, footer)
    return CommandUIRender(page)


