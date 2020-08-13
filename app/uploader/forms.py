import re

from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed
from wtforms import FileField, SelectField, StringField
from wtforms.ext.sqlalchemy.fields import QuerySelectField
from wtforms.validators import DataRequired, StopValidation

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

    def validate(self):
        rv = FlaskForm.validate(self)
        if not rv:
            return False

        pipeline = Pipeline.query.filter_by(
            organisation=self.format(self.organisation.data), dataset=self.format(self.dataset.data)
        ).first()
        if pipeline is not None:
            self.errors['non_field_errors'] = [
                'Pipeline for organisation and dataset already exists'
            ]
            self.organisation.errors.append('Please update')
            self.dataset.errors.append('Please update')
            return False
        return True


class PipelineSelectForm(FlaskForm):
    pipeline = QuerySelectField(query_factory=lambda: Pipeline.query.all())


class DataFileForm(FlaskForm):
    csv_file = FileField(
        'CSV File',
        validators=[DataRequired(), FileAllowed(['csv'], 'Unsupported file type')],
        render_kw={'class': 'govuk-input'},
    )


class VerifyDataFileForm(FlaskForm):
    proceed = SelectField(
        choices=[
            (YES, 'Yes and start processing'),
            (NO, 'No and return back to the beginning to try again'),
        ],
        label='Do the contents of the file look correct?',
        validators=[DataRequired()],
    )


class RestoreVersionForm(FlaskForm):
    proceed = SelectField(
        choices=[(YES, 'Restore'), (NO, 'Cancel')],
        label='Are you sure you want to restore this version of the data',
        validators=[DataRequired()],
    )
