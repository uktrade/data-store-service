import re

from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed
from wtforms import FileField, SelectField, StringField
from wtforms.validators import DataRequired, StopValidation, ValidationError
from wtforms_sqlalchemy.fields import QuerySelectField

from app.constants import NO, YES
from app.db.models.internal import Pipeline


class DBIdentifierValidator:
    def __call__(self, form, field):
        if not re.match(r'^[a-zA-Z0-9_\.]+$', field.data.strip()):
            # Intentionally a bit vague because only Data Workspace users will see this right now.
            # It should also provide a prompt for us to revisit this before releasing to more
            # users, at which point we should be enforcing our data standards more automatically.
            raise StopValidation("Please choose a name that meets our dataset naming conventions.")


class PipelineForm(FlaskForm):
    organisation = StringField(
        'Organisation',
        validators=[DataRequired(), DBIdentifierValidator()],
        render_kw={'class': 'govuk-input'},
    )
    dataset = StringField(
        'Dataset',
        validators=[DataRequired(), DBIdentifierValidator()],
        render_kw={'class': 'govuk-input'},
    )

    def format(self, field):
        return field.lower().replace('.', '_')

    def validate(self, extra_validators=None):
        rv = FlaskForm.validate(self, extra_validators=extra_validators)
        if not rv:
            return False

        pipeline = Pipeline.query.filter_by(
            organisation=self.format(self.organisation.data), dataset=self.format(self.dataset.data)
        ).first()
        if pipeline is not None:
            self.form_errors.append('Pipeline for organisation and dataset already exists')
            self.organisation.errors.append('Please update')
            self.dataset.errors.append('Please update')
            return False
        return True


class PipelineSelectForm(FlaskForm):
    pipeline = QuerySelectField(
        query_factory=lambda: Pipeline.query.all(),
        validators=[DataRequired(message="Select one of the existing pipelines to upload data to")],
    )


class DataFileForm(FlaskForm):
    csv_file = FileField(
        'CSV File',
        validators=[
            DataRequired(message="Select a CSV data file to upload"),
            FileAllowed(['csv'], 'Select a CSV data file to upload'),
        ],
        render_kw={'class': 'govuk-input', "required": False},
    )


class ValidateContentsSelectField(SelectField):
    def pre_validate(self, form):
        try:
            super().pre_validate(form)
        except ValidationError:
            raise StopValidation("Confirm or reject the data file contents")


class VerifyDataFileForm(FlaskForm):
    proceed = ValidateContentsSelectField(
        choices=[
            (YES, 'Yes and start processing'),
            (NO, 'No and return back to the beginning to try again'),
        ],
        label='Do the contents of the file look correct?',
        validators=[DataRequired()],
    )


class RestoreVersionForm(FlaskForm):
    proceed = ValidateContentsSelectField(
        choices=[(YES, 'Restore'), (NO, 'Cancel')],
        label='Are you sure you want to restore this version of the data',
        validators=[DataRequired()],
    )
