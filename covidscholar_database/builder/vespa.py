import os
import itertools
from mongoengine import connect, DoesNotExist
from covidscholar_database.parse.elsevier import UnparsedElsevierDocument
from covidscholar_database.parse.google_form_submissions import UnparsedGoogleFormSubmissionDocument
from covidscholar_database.parse.litcovid import UnparsedLitCovidCrossrefDocument, UnparsedLitCovidPubmedXMLDocument
from covidscholar_database.parse.biorxiv import UnparsedBiorxivDocument
from covidscholar_database.parse.cord19 import UnparsedCORD19CustomDocument, UnparsedCORD19CommDocument, \
    UnparsedCORD19NoncommDocument, UnparsedCORD19XrxivDocument
from covidscholar_database.parse.pho import UnparsedPHODocument
from covidscholar_database.parse.dimensions import UnparsedDimensionsDataDocument, UnparsedDimensionsPubDocument, \
    UnparsedDimensionsTrialDocument
from covidscholar_database.parse.lens_patents import UnparsedLensDocument
from joblib import Parallel, delayed
from covidscholar_database.build.entries import build_entries


def init_mongoengine():
    connect(db=os.getenv("COVID_DB"),
            name=os.getenv("COVID_DB"),
            host=os.getenv("COVID_HOST"),
            username=os.getenv("COVID_USER"),
            password=os.getenv("COVID_PASS"),
            authentication_source=os.getenv("COVID_DB"),
            )

unparsed_collection_list = [UnparsedDimensionsDataDocument,
                            UnparsedDimensionsPubDocument,
                            UnparsedDimensionsTrialDocument,
                            UnparsedLensDocument,
                            UnparsedGoogleFormSubmissionDocument,
                            UnparsedPHODocument,
                            UnparsedElsevierDocument,
                            UnparsedCORD19CustomDocument,
                            UnparsedCORD19CommDocument,
                            UnparsedCORD19NoncommDocument,
                            UnparsedCORD19XrxivDocument,
                            UnparsedBiorxivDocument,
                            UnparsedLitCovidCrossrefDocument,
                            UnparsedLitCovidPubmedXMLDocument,
                            ]


def parse_document(document):
    try:
        parsed_document = document.parsed_document
    except DoesNotExist:
        parsed_document = None

    fresh_source = document.last_updated > parsed_document._bt
    new_parser = parsed_document.version < parsed_document.latest_version

    if parsed_document is None or fresh_source or new_parser:
        if parsed_document is None:
            parsed_document = document.parse()
        else:
            new_doc = document.parse()
            parsed_document.delete()
            parsed_document = new_doc
        document.parsed_document = parsed_document
        parsed_document.find_missing_ids()
        parsed_document.save()
        document.save()


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def parse_documents(documents):
    init_mongoengine()
    # print("parsing")
    for document in documents:
        parse_document(document)
    print('parsed')


if __name__ == "__main__":

    init_mongoengine()

    with Parallel(n_jobs=32) as parallel:
        parallel(delayed(parse_documents)(document) for collection in unparsed_collection_list for document in
                 grouper(500, collection.objects))

    build_entries()
