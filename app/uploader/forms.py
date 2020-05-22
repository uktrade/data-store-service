from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed
from wtforms import FileField, SelectField, StringField
from wtforms.ext.sqlalchemy.fields import QuerySelectField
from wtforms.validators import DataRequired

from app.constants import NO, YES
from app.db.models.internal import Pipeline


class PipelineForm(FlaskForm):
    organisation = StringField(
        'Organisation', validators=[DataRequired()], render_kw={'class': 'govuk-input'}
    )
    dataset = StringField(
        'Dataset', validators=[DataRequired()], render_kw={'class': 'govuk-input'}
    )

    def validate(self):
        rv = FlaskForm.validate(self)
        if not rv:
            return False

        pipeline = Pipeline.query.filter_by(
            organisation=self.organisation.data, dataset=self.dataset.data
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
        label='Does the contents of the file look correct?',
        validators=[DataRequired()],
    )


class RestoreVersionForm(FlaskForm):
    proceed = SelectField(
        choices=[
            (YES, 'Restore'),
            (NO, 'Cancel'),
        ],
        label='Are you sure you want to restore this version of the data',
        validators=[DataRequired()],
    )
