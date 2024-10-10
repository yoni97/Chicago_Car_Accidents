from flask import Blueprint, jsonify, request


injury_bp = Blueprint('injury_bp', __name__)

# @injury_bp.route('/accidents/str:area', methods=['GET'])
#